from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..utils import atomic_write_json_utf8


@dataclass(frozen=True)
class TableCheckpoint:
    watermark: str | None
    rows: int
    sha256: str
    connector_id: str
    updated_at: str


class CheckpointStore:
    SCHEMA_VERSION = 1

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, TableCheckpoint]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Estado de ingesta ilegible: {self.path}") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != self.SCHEMA_VERSION:
            raise ValueError("Versión de checkpoints no compatible")
        tables = payload.get("tables")
        if not isinstance(tables, dict):
            raise ValueError("Checkpoints sin objeto tables válido")

        checkpoints: dict[str, TableCheckpoint] = {}
        for table, raw_state in tables.items():
            if not isinstance(table, str) or not isinstance(raw_state, dict):
                raise ValueError("Checkpoint de tabla inválido")
            try:
                checkpoints[table] = TableCheckpoint(
                    watermark=raw_state.get("watermark"),
                    rows=int(raw_state["rows"]),
                    sha256=str(raw_state["sha256"]),
                    connector_id=str(raw_state["connector_id"]),
                    updated_at=str(raw_state["updated_at"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"Checkpoint inválido para {table}") from exc
        return checkpoints

    def save(self, checkpoints: dict[str, TableCheckpoint]) -> None:
        payload = {
            "schema_version": self.SCHEMA_VERSION,
            "tables": {name: asdict(checkpoints[name]) for name in sorted(checkpoints)},
        }
        atomic_write_json_utf8(self.path, payload)


class ExclusiveRunLock:
    """Bloqueo interproceso con recuperación explícita de locks caducados."""

    def __init__(self, path: Path, *, run_id: str, max_age: timedelta = timedelta(hours=2)) -> None:
        self.path = path
        self.run_id = run_id
        self.max_age = max_age
        self._acquired = False

    def __enter__(self) -> ExclusiveRunLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._remove_stale_lock()
        payload = {
            "run_id": self.run_id,
            "pid": os.getpid(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as exc:
            raise RuntimeError(f"Ya existe una ejecución activa: {self.path}") from exc
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
            handle.write("\n")
        self._acquired = True
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._acquired:
            self.path.unlink(missing_ok=True)
            self._acquired = False

    def _remove_stale_lock(self) -> None:
        if not self.path.exists():
            return
        age_seconds = datetime.now(timezone.utc).timestamp() - self.path.stat().st_mtime
        if age_seconds > self.max_age.total_seconds():
            self.path.unlink(missing_ok=True)


def checkpoint_to_since(checkpoint: TableCheckpoint | None, *, lookback: timedelta) -> str | None:
    if checkpoint is None or checkpoint.watermark is None:
        return None
    watermark = datetime.fromisoformat(checkpoint.watermark.replace("Z", "+00:00"))
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)
    return (watermark.astimezone(timezone.utc) - lookback).isoformat()


def read_json_object(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Se esperaba un objeto JSON: {path}")
    return payload
