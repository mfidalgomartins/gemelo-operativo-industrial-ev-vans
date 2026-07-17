from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from gemelo_operativo_ev.ingestion.connectors import ConnectorRegistry, CsvDirectoryConnector
from gemelo_operativo_ev.ingestion.contracts import SOURCE_CONTRACTS, SourceSystem
from gemelo_operativo_ev.ingestion.service import IngestionMode, run_ingestion


def _registry(source_dir: Path) -> ConnectorRegistry:
    connector = CsvDirectoryConnector(source_dir)
    return ConnectorRegistry({system: connector for system in SourceSystem})


def _turnos() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "fecha": "2026-01-01",
                "turno": "A",
                "headcount_proxy": 100,
                "absentismo_proxy": 0.03,
                "productividad_turno_indice": 0.98,
                "presion_operativa_indice": 0.30,
                "overtime_flag": 0,
            },
            {
                "fecha": "2026-01-02",
                "turno": "A",
                "headcount_proxy": 98,
                "absentismo_proxy": 0.04,
                "productividad_turno_indice": 0.96,
                "presion_operativa_indice": 0.35,
                "overtime_flag": 0,
            },
        ]
    )


@pytest.mark.integration
def test_source_contracts_validate_canonical_snapshot() -> None:
    raw_dir = Path("data/raw/ev_factory")
    for contract in SOURCE_CONTRACTS.values():
        contract.validate(pd.read_csv(raw_dir / contract.filename))


def test_incremental_ingestion_is_idempotent_and_updates_recent_keys(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    state_dir = tmp_path / "state"
    lineage_path = tmp_path / "lineage" / "latest.json"
    source_dir.mkdir()
    source = _turnos()
    source.to_csv(source_dir / "turnos.csv", index=False)

    first = run_ingestion(
        _registry(source_dir),
        target_dir=target_dir,
        state_dir=state_dir,
        lineage_path=lineage_path,
        mode=IngestionMode.FULL,
        tables=("turnos",),
    )
    assert first.status == "PASS"
    assert first.tables[0].rows_after == 2

    source.loc[source["fecha"] == "2026-01-02", "headcount_proxy"] = 105
    source.loc[len(source)] = {
        "fecha": "2026-01-03",
        "turno": "A",
        "headcount_proxy": 101,
        "absentismo_proxy": 0.02,
        "productividad_turno_indice": 0.99,
        "presion_operativa_indice": 0.25,
        "overtime_flag": 0,
    }
    source.to_csv(source_dir / "turnos.csv", index=False)

    second = run_ingestion(
        _registry(source_dir),
        target_dir=target_dir,
        state_dir=state_dir,
        lineage_path=lineage_path,
        mode=IngestionMode.INCREMENTAL,
        tables=("turnos",),
    )
    result = pd.read_csv(target_dir / "turnos.csv")
    assert second.tables[0].rows_after == 3
    assert result.loc[result["fecha"] == "2026-01-02", "headcount_proxy"].item() == 105
    assert not list(target_dir.glob(".ingestion-*"))
    assert not (state_dir.parent / "operation.lock").exists()
    assert json.loads(lineage_path.read_text(encoding="utf-8"))["status"] == "PASS"

    third = run_ingestion(
        _registry(source_dir),
        target_dir=target_dir,
        state_dir=state_dir,
        lineage_path=lineage_path,
        mode=IngestionMode.INCREMENTAL,
        tables=("turnos",),
    )
    assert third.tables[0].rows_after == 3


def test_failed_ingestion_does_not_replace_published_table(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()
    target_dir.mkdir()
    valid = _turnos()
    valid.to_csv(source_dir / "turnos.csv", index=False)
    valid.to_csv(target_dir / "turnos.csv", index=False)
    original = (target_dir / "turnos.csv").read_bytes()
    valid.drop(columns=["turno"]).to_csv(source_dir / "turnos.csv", index=False)

    with pytest.raises(ValueError, match="esquema inválido"):
        run_ingestion(
            _registry(source_dir),
            target_dir=target_dir,
            state_dir=tmp_path / "state",
            lineage_path=tmp_path / "lineage.json",
            mode=IngestionMode.FULL,
            tables=("turnos",),
        )

    assert (target_dir / "turnos.csv").read_bytes() == original


def test_contract_rejects_duplicate_primary_keys() -> None:
    frame = pd.concat([_turnos(), _turnos().iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="clave duplicada"):
        SOURCE_CONTRACTS["turnos"].validate(frame)
