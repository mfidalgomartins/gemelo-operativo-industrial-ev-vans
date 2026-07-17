from __future__ import annotations

import json
import logging
import os
import platform
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from . import __version__
from .utils import atomic_write_json_utf8

LOGGER = logging.getLogger("gemelo_operativo_ev")


def configure_json_logging(level: int = logging.INFO) -> None:
    if LOGGER.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(level)
    LOGGER.propagate = False


def _log_event(event: str, **fields: object) -> None:
    configure_json_logging()
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": "INFO",
        "event": event,
        **fields,
    }
    LOGGER.info(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str))


@dataclass(frozen=True)
class StageRecord:
    name: str
    status: str
    duration_seconds: float
    started_at: str
    finished_at: str
    error_type: str | None = None


@dataclass(frozen=True)
class SlaResult:
    status: str
    max_run_seconds: float
    actual_run_seconds: float
    release_approved: bool
    violations: tuple[str, ...]


def _positive_float_from_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} debe ser numérico") from exc
    if value <= 0:
        raise ValueError(f"{name} debe ser positivo")
    return value


def evaluate_sla(*, duration_seconds: float, release_approved: bool) -> SlaResult:
    max_run_seconds = _positive_float_from_env("EV_TWIN_SLA_MAX_RUN_SECONDS", 300.0)
    violations: list[str] = []
    if duration_seconds > max_run_seconds:
        violations.append("pipeline_duration")
    if not release_approved:
        violations.append("release_gate")
    return SlaResult(
        status="PASS" if not violations else "FAIL",
        max_run_seconds=max_run_seconds,
        actual_run_seconds=duration_seconds,
        release_approved=release_approved,
        violations=tuple(violations),
    )


class PipelineRunRecorder:
    def __init__(self, manifest_path: Path) -> None:
        self.manifest_path = manifest_path
        self.run_id = uuid.uuid4().hex
        self.started_at = datetime.now(timezone.utc)
        self._started_monotonic = time.monotonic()
        self._stages: list[StageRecord] = []

    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        stage_started = datetime.now(timezone.utc)
        stage_monotonic = time.monotonic()
        _log_event("pipeline_stage_started", run_id=self.run_id, stage=name)
        try:
            yield
        except Exception as exc:
            stage_finished = datetime.now(timezone.utc)
            record = StageRecord(
                name=name,
                status="FAIL",
                duration_seconds=round(time.monotonic() - stage_monotonic, 6),
                started_at=stage_started.isoformat(),
                finished_at=stage_finished.isoformat(),
                error_type=type(exc).__name__,
            )
            self._stages.append(record)
            _log_event(
                "pipeline_stage_failed",
                run_id=self.run_id,
                stage=name,
                duration_seconds=record.duration_seconds,
                error_type=record.error_type,
            )
            raise
        else:
            stage_finished = datetime.now(timezone.utc)
            record = StageRecord(
                name=name,
                status="PASS",
                duration_seconds=round(time.monotonic() - stage_monotonic, 6),
                started_at=stage_started.isoformat(),
                finished_at=stage_finished.isoformat(),
            )
            self._stages.append(record)
            _log_event(
                "pipeline_stage_completed",
                run_id=self.run_id,
                stage=name,
                duration_seconds=record.duration_seconds,
            )

    def finish(self, *, status: str, release_approved: bool, error_type: str | None = None) -> dict[str, object]:
        if status not in {"PASS", "FAIL"}:
            raise ValueError("status debe ser PASS o FAIL")
        finished_at = datetime.now(timezone.utc)
        duration = round(time.monotonic() - self._started_monotonic, 6)
        sla = evaluate_sla(duration_seconds=duration, release_approved=release_approved)
        payload: dict[str, object] = {
            "schema_version": 1,
            "run_id": self.run_id,
            "status": status,
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": duration,
            "package_version": __version__,
            "python_version": platform.python_version(),
            "stages": [asdict(stage) for stage in self._stages],
            "sla": asdict(sla),
            "error_type": error_type,
        }
        atomic_write_json_utf8(self.manifest_path, payload)
        _log_event(
            "pipeline_run_finished",
            run_id=self.run_id,
            status=status,
            duration_seconds=duration,
            sla_status=sla.status,
        )
        return payload
