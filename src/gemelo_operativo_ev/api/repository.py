from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


class DataUnavailableError(RuntimeError):
    pass


def _read_json(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise DataUnavailableError(f"Artefacto no disponible: {path.name}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DataUnavailableError(f"Artefacto inválido: {path.name}") from exc
    if not isinstance(payload, dict):
        raise DataUnavailableError(f"Artefacto sin objeto raíz: {path.name}")
    return payload


def read_status(*, reports_dir: Path, runtime_state_dir: Path) -> dict[str, object]:
    release = _read_json(reports_dir / "release_readiness.json")
    manifest_path = runtime_state_dir / "observability" / "latest_pipeline_run.json"
    pipeline = _read_json(manifest_path) if manifest_path.is_file() else {}
    sla = pipeline.get("sla") if isinstance(pipeline.get("sla"), dict) else {}
    return {
        "release_status": str(release.get("status", "UNKNOWN")),
        "release_grade": str(release.get("release_grade", "unknown")),
        "dashboard_version": release.get("dashboard_version"),
        "pipeline_status": str(pipeline.get("status", "UNKNOWN")),
        "pipeline_run_id": pipeline.get("run_id"),
        "pipeline_duration_seconds": pipeline.get("duration_seconds"),
        "sla_status": str(sla.get("status", "UNKNOWN")),
    }


def read_kpi_snapshot(processed_dir: Path) -> dict[str, object]:
    path = processed_dir / "ev_factory" / "kpi_operativos.csv"
    if not path.is_file():
        raise DataUnavailableError("KPI canónico no disponible")
    frame = pd.read_csv(path)
    if len(frame) != 1:
        raise DataUnavailableError("KPI canónico debe contener exactamente una fila")
    columns = (
        "total_ordenes",
        "throughput_real",
        "throughput_gap",
        "share_ev",
        "ocupacion_pico_patio",
        "utilizacion_media_cargadores",
        "tiempo_medio_espera_carga_min",
        "vehiculos_no_ready",
        "ratio_salida_retrasada",
        "score_readiness_global",
        "causa_principal_cuello",
        "area_mayor_perdida_throughput",
    )
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise DataUnavailableError(f"KPI canónico incompleto: {missing}")
    return frame.loc[0, list(columns)].to_dict()


def read_priorities(processed_dir: Path, *, limit: int) -> list[dict[str, object]]:
    path = processed_dir / "ev_factory" / "operational_prioritization_table.csv"
    if not path.is_file():
        raise DataUnavailableError("Ranking operativo no disponible")
    frame = pd.read_csv(path)
    columns = (
        "area",
        "operational_priority_index",
        "main_risk_driver",
        "recommended_action",
        "area_priority_tier",
    )
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise DataUnavailableError(f"Ranking operativo incompleto: {missing}")
    selected = frame.sort_values("operational_priority_index", ascending=False).head(limit)
    return selected.loc[:, list(columns)].to_dict(orient="records")


def read_lineage(runtime_state_dir: Path) -> dict[str, object]:
    path = runtime_state_dir / "lineage" / "latest_ingestion.json"
    if not path.is_file():
        return {"available": False, "table_count": 0}
    payload = _read_json(path)
    tables = payload.get("tables")
    return {
        "available": True,
        "run_id": payload.get("run_id"),
        "status": payload.get("status"),
        "mode": payload.get("mode"),
        "finished_at": payload.get("finished_at"),
        "table_count": len(tables) if isinstance(tables, list) else 0,
    }
