from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from gemelo_operativo_ev.api import app as app_module
from gemelo_operativo_ev.api.app import create_app
from gemelo_operativo_ev.api.repository import (
    DataUnavailableError,
    read_kpi_snapshot,
    read_lineage,
    read_priorities,
    read_status,
)
from gemelo_operativo_ev.api.server import _positive_int
from gemelo_operativo_ev.api.server import main as server_main
from gemelo_operativo_ev.api.settings import ApiSettings

VIEWER_TOKEN = "v" * 32
OPERATOR_TOKEN = "o" * 32


def _settings() -> ApiSettings:
    return ApiSettings(
        environment="test",
        trusted_hosts=("testserver",),
        viewer_token=VIEWER_TOKEN,
        operator_token=OPERATOR_TOKEN,
    )


def _write_api_artifacts(root: Path) -> tuple[Path, Path, Path]:
    processed = root / "processed"
    reports = root / "reports"
    runtime = root / "runtime"
    (processed / "ev_factory").mkdir(parents=True)
    reports.mkdir()
    (runtime / "observability").mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "total_ordenes": 100,
                "throughput_real": 98,
                "throughput_gap": -2,
                "share_ev": 0.4,
                "ocupacion_pico_patio": 0.7,
                "utilizacion_media_cargadores": 0.6,
                "tiempo_medio_espera_carga_min": 12.0,
                "vehiculos_no_ready": 4,
                "ratio_salida_retrasada": 0.1,
                "score_readiness_global": 88.0,
                "causa_principal_cuello": "PATIO",
                "area_mayor_perdida_throughput": "PATIO",
            }
        ]
    ).to_csv(processed / "ev_factory" / "kpi_operativos.csv", index=False)
    pd.DataFrame(
        [
            {
                "area": "PATIO",
                "operational_priority_index": 70.0,
                "main_risk_driver": "Riesgo de patio",
                "recommended_action": "reducir permanencia",
                "area_priority_tier": "estabilizar",
            }
        ]
    ).to_csv(processed / "ev_factory" / "operational_prioritization_table.csv", index=False)
    (reports / "release_readiness.json").write_text(
        json.dumps({"status": "PASS", "release_grade": "decision-support only", "dashboard_version": "ev-123"}),
        encoding="utf-8",
    )
    (runtime / "observability" / "latest_pipeline_run.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "run_id": "run-1",
                "duration_seconds": 30.0,
                "sla": {"status": "PASS"},
            }
        ),
        encoding="utf-8",
    )
    return processed, reports, runtime


def test_api_is_fail_closed_and_exposes_typed_read_models(tmp_path: Path, monkeypatch) -> None:
    processed, reports, runtime = _write_api_artifacts(tmp_path)
    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "OUTPUT_REPORTS_DIR", reports)
    monkeypatch.setattr(app_module, "RUNTIME_STATE_DIR", runtime)
    monkeypatch.setattr(
        app_module,
        "run_release_gate",
        lambda: SimpleNamespace(approved=True, release_grade="decision-support only", reason="Publicación apta"),
    )
    client = TestClient(create_app(_settings()), raise_server_exceptions=False)

    live = client.get("/health/live")
    assert live.status_code == 200
    assert live.headers["x-content-type-options"] == "nosniff"
    assert client.get("/health/ready").json()["status"] == "ready"
    assert client.get("/v1/kpis").status_code == 401

    viewer_headers = {"Authorization": f"Bearer {VIEWER_TOKEN}"}
    kpis = client.get("/v1/kpis", headers=viewer_headers)
    assert kpis.status_code == 200
    assert kpis.json()["total_ordenes"] == 100
    assert client.get("/v1/priorities?limit=1", headers=viewer_headers).json()["count"] == 1
    assert client.post("/v1/operator/release-check", headers=viewer_headers).status_code == 403

    operator_headers = {"Authorization": f"Bearer {OPERATOR_TOKEN}"}
    release = client.post("/v1/operator/release-check", headers=operator_headers)
    assert release.status_code == 200
    assert release.json()["approved"] is True
    assert client.get("/docs").status_code == 404


def test_api_rejects_untrusted_hosts_and_oversized_requests() -> None:
    client = TestClient(create_app(_settings()), raise_server_exceptions=False)
    assert client.get("/health/live", headers={"Host": "attacker.example"}).status_code == 400
    response = client.get("/health/live", headers={"Content-Length": "9999999"})
    assert response.status_code == 413


def test_api_settings_reject_weak_or_shared_tokens() -> None:
    try:
        ApiSettings(
            environment="production",
            trusted_hosts=("api.example.com",),
            viewer_token="short",
            operator_token="short",
        )
    except ValueError as exc:
        assert "32 caracteres" in str(exc)
    else:  # pragma: no cover - guardia explícita
        raise AssertionError("Se aceptaron tokens débiles")


def test_api_settings_load_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("EV_TWIN_ENV", "test")
    monkeypatch.setenv("EV_TWIN_TRUSTED_HOSTS", "testserver,api.example.net")
    monkeypatch.setenv("EV_TWIN_VIEWER_TOKEN", VIEWER_TOKEN)
    monkeypatch.setenv("EV_TWIN_OPERATOR_TOKEN", OPERATOR_TOKEN)
    monkeypatch.setenv("EV_TWIN_DOCS_ENABLED", "true")
    monkeypatch.setenv("EV_TWIN_MAX_REQUEST_BYTES", "2048")

    settings = ApiSettings.from_env()

    assert settings.trusted_hosts == ("testserver", "api.example.net")
    assert settings.docs_enabled is True
    assert settings.max_request_bytes == 2048


def test_api_settings_reject_production_docs_and_invalid_limits() -> None:
    with pytest.raises(ValueError, match="documentación interactiva"):
        ApiSettings(
            environment="production",
            trusted_hosts=("api.example.net",),
            viewer_token=VIEWER_TOKEN,
            operator_token=OPERATOR_TOKEN,
            docs_enabled=True,
        )
    with pytest.raises(ValueError, match="debe ser positivo"):
        ApiSettings(
            environment="test",
            trusted_hosts=("testserver",),
            viewer_token=VIEWER_TOKEN,
            operator_token=OPERATOR_TOKEN,
            max_request_bytes=0,
        )


def test_api_repository_handles_status_lineage_and_invalid_data(tmp_path: Path) -> None:
    processed, reports, runtime = _write_api_artifacts(tmp_path)

    status = read_status(reports_dir=reports, runtime_state_dir=runtime)
    assert status["release_status"] == "PASS"
    assert read_lineage(runtime) == {"available": False, "table_count": 0}

    lineage_path = runtime / "lineage" / "latest_ingestion.json"
    lineage_path.parent.mkdir()
    lineage_path.write_text(
        json.dumps(
            {
                "run_id": "ingest-1",
                "status": "PASS",
                "mode": "incremental",
                "finished_at": "2026-01-01T00:00:00+00:00",
                "tables": [{"table": "turnos"}],
            }
        ),
        encoding="utf-8",
    )
    assert read_lineage(runtime)["table_count"] == 1
    assert read_kpi_snapshot(processed)["total_ordenes"] == 100
    assert read_priorities(processed, limit=1)[0]["area"] == "PATIO"

    (reports / "release_readiness.json").write_text("[]", encoding="utf-8")
    with pytest.raises(DataUnavailableError, match="objeto raíz"):
        read_status(reports_dir=reports, runtime_state_dir=runtime)


def test_api_reports_invalid_content_length_and_not_ready(tmp_path: Path, monkeypatch) -> None:
    runtime = tmp_path / "runtime"
    reports = tmp_path / "reports"
    monkeypatch.setattr(app_module, "OUTPUT_REPORTS_DIR", reports)
    monkeypatch.setattr(app_module, "RUNTIME_STATE_DIR", runtime)
    client = TestClient(create_app(_settings()), raise_server_exceptions=False)

    assert client.get("/health/live", headers={"Content-Length": "invalid"}).status_code == 400
    assert client.get("/health/ready").json()["status"] == "not_ready"


def test_api_server_validates_environment_and_delegates_to_uvicorn(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_run(target: str, **kwargs) -> None:
        calls.append((target, kwargs))

    import uvicorn

    monkeypatch.setattr(uvicorn, "run", fake_run)
    monkeypatch.setenv("EV_TWIN_API_HOST", "127.0.0.1")
    monkeypatch.setenv("EV_TWIN_API_PORT", "9000")
    monkeypatch.setenv("EV_TWIN_API_WORKERS", "2")

    server_main()

    assert calls[0][0] == "gemelo_operativo_ev.api.app:create_app"
    assert calls[0][1]["port"] == 9000
    assert calls[0][1]["workers"] == 2
    assert calls[0][1]["proxy_headers"] is False

    monkeypatch.setenv("EV_TWIN_API_HOST", "bad/host")
    with pytest.raises(ValueError, match="HOST"):
        server_main()
    monkeypatch.setenv("EV_TWIN_API_HOST", "127.0.0.1")
    monkeypatch.setenv("EV_TWIN_API_PORT", "zero")
    with pytest.raises(ValueError, match="debe ser entero"):
        server_main()
    assert _positive_int("EV_TWIN_UNSET_TEST_VALUE", 3) == 3
    os.environ.pop("EV_TWIN_UNSET_TEST_VALUE", None)
