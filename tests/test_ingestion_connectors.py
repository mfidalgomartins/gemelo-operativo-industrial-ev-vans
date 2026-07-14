from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pandas as pd
import pytest

from gemelo_operativo_ev.ingestion.connectors import (
    ConnectorRegistry,
    CsvDirectoryConnector,
    HttpJsonConnector,
)
from gemelo_operativo_ev.ingestion.contracts import SOURCE_CONTRACTS, SourceSystem
from gemelo_operativo_ev.ingestion.factory import build_connector_registry
from gemelo_operativo_ev.ingestion.state import (
    CheckpointStore,
    ExclusiveRunLock,
    TableCheckpoint,
    checkpoint_to_since,
)


def _turno(fecha: str, headcount: int = 100) -> dict[str, object]:
    return {
        "fecha": fecha,
        "turno": "A",
        "headcount_proxy": headcount,
        "absentismo_proxy": 0.03,
        "productividad_turno_indice": 0.98,
        "presion_operativa_indice": 0.30,
        "overtime_flag": 0,
    }


def test_csv_connector_applies_watermark_and_reports_safe_source(tmp_path: Path) -> None:
    pd.DataFrame([_turno("2026-01-01"), _turno("2026-01-03")]).to_csv(tmp_path / "turnos.csv", index=False)
    connector = CsvDirectoryConnector(tmp_path, name="mes_drop")

    batch = connector.extract(SOURCE_CONTRACTS["turnos"], since="2026-01-02T00:00:00+00:00")

    assert connector.connector_id == "mes_drop"
    assert batch.source_ref == "csv://turnos.csv"
    assert batch.frame["fecha"].tolist() == ["2026-01-03"]
    assert batch.extracted_at.tzinfo is timezone.utc


def test_csv_connector_rejects_missing_source(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Falta fuente"):
        CsvDirectoryConnector(tmp_path).extract(SOURCE_CONTRACTS["turnos"])


def test_http_connector_paginates_with_fixed_host(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        cursor = request.url.params.get("cursor")
        payload = (
            {"items": [_turno("2026-01-01")], "next_cursor": "page-2"}
            if cursor is None
            else {"items": [_turno("2026-01-02", 101)], "next_cursor": None}
        )
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    real_client = httpx.Client

    def client_factory(**kwargs):
        return real_client(transport=transport, **kwargs)

    monkeypatch.setattr(httpx, "Client", client_factory)
    connector = HttpJsonConnector(
        name="mes_api",
        base_url="https://integration.example.net/api/",
        endpoints={"turnos": "v1/shifts"},
        allowed_hosts=frozenset({"integration.example.net"}),
        bearer_token="t" * 32,
    )

    batch = connector.extract(SOURCE_CONTRACTS["turnos"], since="2025-12-31T00:00:00+00:00")

    assert len(batch.frame) == 2
    assert batch.source_ref == "https://integration.example.net/turnos"
    assert len(requests) == 2
    assert requests[0].headers["authorization"] == f"Bearer {'t' * 32}"
    assert requests[1].url.params["cursor"] == "page-2"


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"base_url": "http://integration.example.net"}, "requieren HTTPS"),
        ({"base_url": "https://other.example.net"}, "allowlist"),
        ({"bearer_token": "short"}, "32 caracteres"),
        ({"endpoints": {"turnos": "../secret"}}, "ruta relativa segura"),
        ({"max_pages": 0}, "deben ser positivos"),
    ],
)
def test_http_connector_rejects_unsafe_configuration(overrides: dict[str, object], message: str) -> None:
    arguments: dict[str, object] = {
        "name": "mes_api",
        "base_url": "https://integration.example.net",
        "endpoints": {"turnos": "v1/shifts"},
        "allowed_hosts": frozenset({"integration.example.net"}),
        "bearer_token": "t" * 32,
    }
    arguments.update(overrides)
    with pytest.raises(ValueError, match=message):
        HttpJsonConnector(**arguments)


def test_connector_factory_builds_all_csv_systems(tmp_path: Path) -> None:
    systems = {system.value: {"kind": "csv_directory", "source_dir": f"drop/{system.value}"} for system in SourceSystem}
    config = tmp_path / "connectors.json"
    config.write_text(json.dumps({"schema_version": 1, "systems": systems}), encoding="utf-8")

    registry = build_connector_registry(config)

    for system in SourceSystem:
        connector = registry.connectors[system]
        assert isinstance(connector, CsvDirectoryConnector)
        assert connector.source_dir == (tmp_path / "drop" / system.value).resolve()


def test_connector_factory_builds_complete_http_systems(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EV_TWIN_SOURCE_TOKEN", "s" * 32)
    systems = {}
    for system in SourceSystem:
        endpoints = {
            contract.name: f"v1/{contract.name}"
            for contract in SOURCE_CONTRACTS.values()
            if contract.source_system is system
        }
        systems[system.value] = {
            "kind": "http_json",
            "base_url": "https://integration.example.net/api/",
            "allowed_hosts": ["integration.example.net"],
            "token_env": "EV_TWIN_SOURCE_TOKEN",
            "endpoints": endpoints,
        }
    config = tmp_path / "connectors.json"
    config.write_text(json.dumps({"schema_version": 1, "systems": systems}), encoding="utf-8")

    registry = build_connector_registry(config)

    assert all(isinstance(connector, HttpJsonConnector) for connector in registry.connectors.values())


def test_connector_factory_rejects_missing_secret_and_endpoint_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("EV_TWIN_MISSING_TOKEN", raising=False)
    systems = {system.value: {"kind": "csv_directory", "source_dir": "drop"} for system in SourceSystem}
    systems[SourceSystem.MES.value] = {
        "kind": "http_json",
        "base_url": "https://integration.example.net",
        "allowed_hosts": ["integration.example.net"],
        "token_env": "EV_TWIN_MISSING_TOKEN",
        "endpoints": {},
    }
    config = tmp_path / "connectors.json"
    config.write_text(json.dumps({"schema_version": 1, "systems": systems}), encoding="utf-8")

    with pytest.raises(ValueError, match="falta la variable de entorno"):
        build_connector_registry(config)

    monkeypatch.setenv("EV_TWIN_MISSING_TOKEN", "s" * 32)
    with pytest.raises(ValueError, match="cobertura de endpoints inválida"):
        build_connector_registry(config)


def test_registry_and_checkpoint_state_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(KeyError, match="No hay conector"):
        ConnectorRegistry({}).for_contract(SOURCE_CONTRACTS["turnos"])

    store = CheckpointStore(tmp_path / "state" / "checkpoints.json")
    assert store.load() == {}
    checkpoint = TableCheckpoint(
        watermark="2026-01-03T00:00:00+00:00",
        rows=3,
        sha256="a" * 64,
        connector_id="mes_drop",
        updated_at=datetime.now(timezone.utc).isoformat(),
    )
    store.save({"turnos": checkpoint})
    assert store.load()["turnos"] == checkpoint
    assert checkpoint_to_since(checkpoint, lookback=timedelta(days=1)) == "2026-01-02T00:00:00+00:00"

    store.path.write_text('{"schema_version": 9}', encoding="utf-8")
    with pytest.raises(ValueError, match="no compatible"):
        store.load()


def test_exclusive_lock_rejects_overlap_and_recovers_stale_file(tmp_path: Path) -> None:
    lock_path = tmp_path / "pipeline.lock"
    with ExclusiveRunLock(lock_path, run_id="first"):
        with pytest.raises(RuntimeError, match="ejecución activa"):
            with ExclusiveRunLock(lock_path, run_id="second"):
                pass
    assert not lock_path.exists()

    lock_path.write_text("stale", encoding="utf-8")
    stale_time = datetime.now(timezone.utc).timestamp() - 10
    os.utime(lock_path, (stale_time, stale_time))
    with ExclusiveRunLock(lock_path, run_id="recovered", max_age=timedelta(seconds=1)):
        assert lock_path.exists()
    assert not lock_path.exists()
