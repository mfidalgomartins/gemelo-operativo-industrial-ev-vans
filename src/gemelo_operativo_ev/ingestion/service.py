from __future__ import annotations

import os
import tempfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path

import pandas as pd

from ..utils import atomic_write_json_utf8, sha256_file
from .connectors import ConnectorRegistry, ExtractBatch
from .contracts import SOURCE_CONTRACTS, TableContract
from .state import CheckpointStore, ExclusiveRunLock, TableCheckpoint, checkpoint_to_since


class IngestionMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass(frozen=True)
class TableIngestionResult:
    table: str
    source_system: str
    connector_id: str
    source_ref: str
    rows_before: int
    rows_extracted: int
    rows_after: int
    watermark: str | None
    sha256: str


@dataclass(frozen=True)
class IngestionResult:
    run_id: str
    mode: str
    started_at: str
    finished_at: str
    status: str
    tables: tuple[TableIngestionResult, ...]


def _read_existing(path: Path, contract: TableContract) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame(columns=list(contract.columns))
    frame = pd.read_csv(path)
    contract.validate(frame, allow_empty=True)
    return contract.project(frame)


def _merge(existing: pd.DataFrame, batch: ExtractBatch, contract: TableContract) -> pd.DataFrame:
    if existing.empty:
        merged = batch.frame.copy()
    elif batch.frame.empty:
        merged = existing.copy()
    else:
        merged = pd.concat([existing, batch.frame], ignore_index=True)
        merged = merged.drop_duplicates(subset=list(contract.primary_key), keep="last")
    merged = merged.sort_values(list(contract.primary_key), kind="stable", na_position="last").reset_index(drop=True)
    contract.validate(merged, allow_empty=True)
    return contract.project(merged)


def _watermark(frame: pd.DataFrame, contract: TableContract) -> str | None:
    if frame.empty or contract.watermark_column is None:
        return None
    values = pd.to_datetime(frame[contract.watermark_column], errors="raise", utc=True)
    return values.max().isoformat()


def run_ingestion(
    registry: ConnectorRegistry,
    *,
    target_dir: Path,
    state_dir: Path,
    lineage_path: Path,
    mode: IngestionMode = IngestionMode.INCREMENTAL,
    tables: tuple[str, ...] | None = None,
    lookback: timedelta = timedelta(days=1),
) -> IngestionResult:
    if not isinstance(mode, IngestionMode):
        raise TypeError("mode debe ser IngestionMode")
    if lookback < timedelta(0):
        raise ValueError("lookback no puede ser negativo")

    selected_names = tables or tuple(SOURCE_CONTRACTS)
    unknown = sorted(set(selected_names) - SOURCE_CONTRACTS.keys())
    if unknown:
        raise ValueError(f"Tablas de ingesta desconocidas: {unknown}")

    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    target_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_store = CheckpointStore(state_dir / "ingestion_checkpoints.json")

    with ExclusiveRunLock(state_dir.parent / "operation.lock", run_id=run_id):
        checkpoints = checkpoint_store.load()
        staged_results: list[tuple[TableContract, Path, ExtractBatch, pd.DataFrame, int]] = []
        with tempfile.TemporaryDirectory(prefix=".ingestion-", dir=target_dir) as temporary_dir:
            staging_dir = Path(temporary_dir)
            for name in selected_names:
                contract = SOURCE_CONTRACTS[name]
                connector = registry.for_contract(contract)
                target_path = target_dir / contract.filename
                existing = _read_existing(target_path, contract)
                since = None
                if mode is IngestionMode.INCREMENTAL:
                    since = checkpoint_to_since(checkpoints.get(name), lookback=lookback)
                batch = connector.extract(contract, since=since)
                merged = batch.frame.copy() if mode is IngestionMode.FULL else _merge(existing, batch, contract)
                contract.validate(merged, allow_empty=False)

                staged_path = staging_dir / contract.filename
                merged.to_csv(staged_path, index=False, lineterminator="\n")
                staged_results.append((contract, staged_path, batch, merged, len(existing)))

            for contract, staged_path, _, _, _ in staged_results:
                os.replace(staged_path, target_dir / contract.filename)

        finished_at = datetime.now(timezone.utc)
        table_results: list[TableIngestionResult] = []
        updated_checkpoints = dict(checkpoints)
        for contract, _, batch, merged, rows_before in staged_results:
            target_path = target_dir / contract.filename
            file_hash = sha256_file(target_path)
            watermark = _watermark(merged, contract)
            result = TableIngestionResult(
                table=contract.name,
                source_system=contract.source_system.value,
                connector_id=registry.for_contract(contract).connector_id,
                source_ref=batch.source_ref,
                rows_before=rows_before,
                rows_extracted=len(batch.frame),
                rows_after=len(merged),
                watermark=watermark,
                sha256=file_hash,
            )
            table_results.append(result)
            updated_checkpoints[contract.name] = TableCheckpoint(
                watermark=watermark,
                rows=len(merged),
                sha256=file_hash,
                connector_id=result.connector_id,
                updated_at=finished_at.isoformat(),
            )

        checkpoint_store.save(updated_checkpoints)
        ingestion_result = IngestionResult(
            run_id=run_id,
            mode=mode.value,
            started_at=started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            status="PASS",
            tables=tuple(table_results),
        )
        atomic_write_json_utf8(
            lineage_path,
            {
                **asdict(ingestion_result),
                "contract_version": 1,
                "decision_grade": "decision-support only",
            },
        )
        return ingestion_result
