from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT
from .dashboard import render_dashboard_html
from .utils import require_columns, write_json_utf8

EV_DIR = DATA_PROCESSED_DIR / "ev_factory"
OFFICIAL_DASHBOARD_NAME = "industrial-ev-operating-command-center.html"

FLOW_REQUIRED_COLUMNS = [
    "fecha_programada",
    "fecha_real",
    "turno",
    "orden_id",
    "vehiculo_id",
    "tipo_propulsion",
    "version_id",
    "planned_to_actual_sequence_gap",
    "total_internal_lead_time_min",
    "yard_wait_time_min",
    "charging_wait_time_min",
    "dispatch_delay_min",
    "readiness_final_flag",
]

YARD_REQUIRED_COLUMNS = [
    "timestamp",
    "zona_patio",
    "yard_occupancy_rate",
    "avg_dwell_time",
    "p95_dwell_time",
    "blocking_rate",
    "non_productive_move_rate",
]

CHARGING_REQUIRED_COLUMNS = [
    "fecha",
    "turno",
    "zona_carga",
    "charger_pressure_score",
    "avg_wait_to_charge",
    "interruption_rate",
    "target_soc_miss_rate",
    "sessions_per_shift",
]

DISPATCH_REQUIRED_COLUMNS = [
    "fecha",
    "turno",
    "tipo_propulsion",
    "vehiculo_id",
    "departed_flag",
    "delayed_flag",
    "readiness_final_flag",
    "dispatch_delay_min",
    "soc_salida_pct",
    "target_soc_salida_pct",
    "causa_retraso",
]

BNECK_REQUIRED_COLUMNS = [
    "fecha",
    "turno",
    "area",
    "severidad_media",
    "impacto_throughput_total",
    "impacto_salida_total",
    "area_stress_score",
    "eventos_cuello",
]

PRIORITIES_REQUIRED_COLUMNS = ["area", "operational_priority_index", "recommended_action"]

SCENARIO_REQUIRED_COLUMNS = [
    "escenario",
    "throughput",
    "espera_carga",
    "ocupacion_pico_patio",
    "riesgo_salida_baja_readiness",
    "estabilidad_operativa",
    "decision_score",
]


@dataclass
class DashboardResult:
    path: str
    version: str
    payload_size_bytes: int


def _read_csv(path: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Falta conjunto de datos para el panel: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def _records(df: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in df.to_dict(orient="records"):
        out: dict[str, object] = {}
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                out[k] = v.strftime("%Y-%m-%d")
            elif isinstance(v, (np.floating, float)):
                out[k] = None if pd.isna(v) else round(float(v), 4)
            elif isinstance(v, (np.integer, int)):
                out[k] = int(v)
            elif pd.isna(v):
                out[k] = None
            else:
                out[k] = str(v)
        rows.append(out)
    return rows


def _remove_non_official_dashboards(output_dir: Path, official_name: str) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    removed: list[str] = []
    for html_file in output_dir.glob("*.html"):
        if html_file.name == official_name:
            continue
        removed.append(str(html_file.relative_to(PROJECT_ROOT)))
        html_file.unlink()
    return removed


def _build_meta(
    flow: pd.DataFrame,
    yard: pd.DataFrame,
    charging: pd.DataFrame,
    dispatch: pd.DataFrame,
    priorities: pd.DataFrame,
    scenarios: pd.DataFrame,
    kpi: pd.DataFrame,
) -> dict[str, object]:
    require_columns(flow, FLOW_REQUIRED_COLUMNS, "vw_vehicle_flow_timeline")
    require_columns(yard, YARD_REQUIRED_COLUMNS, "yard_features")
    require_columns(charging, CHARGING_REQUIRED_COLUMNS, "charging_features")
    require_columns(dispatch, DISPATCH_REQUIRED_COLUMNS, "vw_dispatch_readiness")
    require_columns(priorities, PRIORITIES_REQUIRED_COLUMNS, "operational_prioritization_table")
    require_columns(scenarios, SCENARIO_REQUIRED_COLUMNS, "scenario_table")

    coverage_min = pd.to_datetime(flow["fecha_real"], errors="coerce").min()
    coverage_max = pd.to_datetime(flow["fecha_real"], errors="coerce").max()

    top_area = priorities.sort_values("operational_priority_index", ascending=False).head(1)
    top_scenario = scenarios.sort_values("decision_score", ascending=False).head(1)

    kpi_row = kpi.iloc[0].to_dict() if not kpi.empty else {}
    throughput_plan_calc = int(flow["orden_id"].nunique())
    throughput_real_calc = int(flow["vehiculo_id"].nunique())
    share_ev_calc = float((flow["tipo_propulsion"] == "EV").mean()) if len(flow) else 0.0
    readiness_calc = float(flow["readiness_final_flag"].mean() * 100) if len(flow) else 0.0
    departed = dispatch.loc[dispatch["departed_flag"].astype(bool)]
    delay_rate_calc = float(departed["delayed_flag"].mean()) if not departed.empty else 0.0
    kpi_validation = {
        "throughput_planificado_matches_flow": int(kpi_row.get("throughput_planificado", -1)) == throughput_plan_calc,
        "throughput_real_matches_flow": int(kpi_row.get("throughput_real", -1)) == throughput_real_calc,
        "throughput_gap_matches_components": int(kpi_row.get("throughput_gap", 0))
        == int(kpi_row.get("throughput_real", 0)) - int(kpi_row.get("throughput_planificado", 0)),
        "share_ev_matches_flow": abs(float(kpi_row.get("share_ev", 0.0)) - share_ev_calc) <= 0.02,
        "readiness_matches_flow": abs(float(kpi_row.get("score_readiness_global", 0.0)) - readiness_calc) <= 1e-9,
        "delay_rate_matches_dispatch": abs(float(kpi_row.get("ratio_salida_retrasada", 0.0)) - delay_rate_calc) <= 1e-9,
        "proportions_in_range": all(
            0 <= float(kpi_row.get(col, 0.0)) <= 1
            for col in [
                "share_ev",
                "ocupacion_media_patio",
                "ocupacion_pico_patio",
                "utilizacion_media_cargadores",
                "ratio_salida_retrasada",
            ]
        ),
        "scores_in_range": 0 <= float(kpi_row.get("score_readiness_global", 0.0)) <= 100,
    }

    return {
        "coverage": f"{coverage_min.date()} a {coverage_max.date()}"
        if pd.notna(coverage_min) and pd.notna(coverage_max)
        else "Sin dato",
        "orders": int(flow["orden_id"].nunique()),
        "vehicles": int(flow["vehiculo_id"].nunique()),
        "yard_zones": int(yard["zona_patio"].nunique()),
        "charge_zones": int(charging["zona_carga"].nunique()),
        "kpi_official": kpi_row,
        "kpi_validation": kpi_validation,
        "executive_snapshot": {
            "top_area": str(top_area["area"].iloc[0]) if not top_area.empty else "Sin dato",
            "top_action": str(top_area["recommended_action"].iloc[0]) if not top_area.empty else "Sin dato",
            "top_scenario": str(top_scenario["escenario"].iloc[0]) if not top_scenario.empty else "Sin dato",
        },
    }


def _prepare_datasets(
    flow: pd.DataFrame,
    yard: pd.DataFrame,
    charging: pd.DataFrame,
    dispatch: pd.DataFrame,
    bneck: pd.DataFrame,
    priorities: pd.DataFrame,
    scenarios: pd.DataFrame,
    kpi_readiness: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    require_columns(flow, FLOW_REQUIRED_COLUMNS, "vw_vehicle_flow_timeline")
    require_columns(yard, YARD_REQUIRED_COLUMNS, "yard_features")
    require_columns(charging, CHARGING_REQUIRED_COLUMNS, "charging_features")
    require_columns(dispatch, DISPATCH_REQUIRED_COLUMNS, "vw_dispatch_readiness")
    require_columns(bneck, BNECK_REQUIRED_COLUMNS, "vw_shift_bottleneck_summary")
    require_columns(priorities, PRIORITIES_REQUIRED_COLUMNS, "operational_prioritization_table")
    require_columns(scenarios, SCENARIO_REQUIRED_COLUMNS, "scenario_table")

    f = flow.copy()
    f["fecha_programada"] = pd.to_datetime(f["fecha_programada"], errors="coerce")
    f["fecha_real"] = pd.to_datetime(f["fecha_real"], errors="coerce")
    f["week"] = f["fecha_real"].dt.to_period("W").dt.start_time

    throughput = (
        f.groupby([f["fecha_programada"].dt.date.rename("fecha"), "turno"], as_index=False)
        .agg(throughput_plan=("orden_id", "count"))
        .merge(
            f.groupby([f["fecha_real"].dt.date.rename("fecha"), "turno"], as_index=False).agg(
                throughput_real=("vehiculo_id", "count")
            ),
            on=["fecha", "turno"],
            how="outer",
        )
        .fillna(0)
    )
    throughput["fecha"] = pd.to_datetime(throughput["fecha"])
    throughput["throughput_gap"] = throughput["throughput_real"] - throughput["throughput_plan"]

    ev_share_week = (
        f.groupby("week", as_index=False)
        .agg(
            share_ev=("tipo_propulsion", lambda s: float((s == "EV").mean())),
            throughput_real=("vehiculo_id", "count"),
        )
        .sort_values("week")
    )

    seq_gap = f.groupby([f["fecha_real"].dt.date.rename("fecha"), "turno", "tipo_propulsion"], as_index=False).agg(
        sequence_gap=("planned_to_actual_sequence_gap", "mean"),
        lead_time=("total_internal_lead_time_min", "mean"),
    )
    seq_gap["fecha"] = pd.to_datetime(seq_gap["fecha"])

    lead_version = (
        f.groupby(["version_id", "tipo_propulsion"], as_index=False)
        .agg(
            lead_time=("total_internal_lead_time_min", "mean"),
            throughput=("vehiculo_id", "count"),
            yard_wait=("yard_wait_time_min", "mean"),
            charge_wait=("charging_wait_time_min", "mean"),
            delay=("dispatch_delay_min", "mean"),
        )
        .sort_values("lead_time", ascending=False)
        .head(14)
    )

    flow_prop_daily = f.groupby(
        [f["fecha_real"].dt.date.rename("fecha"), "turno", "tipo_propulsion"], as_index=False
    ).agg(
        lead_time=("total_internal_lead_time_min", "mean"),
        yard_wait=("yard_wait_time_min", "mean"),
        charge_wait=("charging_wait_time_min", "mean"),
        delay=("dispatch_delay_min", "mean"),
        throughput=("vehiculo_id", "count"),
    )
    flow_prop_daily["fecha"] = pd.to_datetime(flow_prop_daily["fecha"])

    y = yard.copy()
    y["timestamp"] = pd.to_datetime(y["timestamp"], errors="coerce")
    y["fecha"] = y["timestamp"].dt.date

    yard_daily = y.groupby(["fecha", "zona_patio"], as_index=False).agg(
        occupancy=("yard_occupancy_rate", "mean"),
        dwell=("avg_dwell_time", "mean"),
        dwell_p95=("p95_dwell_time", "mean"),
        blocking=("blocking_rate", "mean"),
        non_productive=("non_productive_move_rate", "mean"),
    )
    yard_daily["fecha"] = pd.to_datetime(yard_daily["fecha"])

    ch = charging.copy()
    ch["fecha"] = pd.to_datetime(ch["fecha"], errors="coerce")
    ch["slot_utilization"] = np.clip(ch["charger_pressure_score"] / 100, 0, 1.5)

    charge_daily = ch.groupby([ch["fecha"].dt.date.rename("fecha"), "turno", "zona_carga"], as_index=False).agg(
        wait=("avg_wait_to_charge", "mean"),
        utilization=("slot_utilization", "mean"),
        interruption=("interruption_rate", "mean"),
        target_miss=("target_soc_miss_rate", "mean"),
        sessions=("sessions_per_shift", "sum"),
    )
    charge_daily["fecha"] = pd.to_datetime(charge_daily["fecha"])

    d = dispatch.copy()
    d["fecha"] = pd.to_datetime(d["fecha"], errors="coerce")
    d["departed_flag"] = d["departed_flag"].astype(bool)
    d["delayed_departed"] = d["delayed_flag"].astype(bool) & d["departed_flag"]

    dispatch_base = d.groupby([d["fecha"].dt.date.rename("fecha"), "turno", "tipo_propulsion"], as_index=False).agg(
        vehicles=("vehiculo_id", "count"),
        departed_vehicles=("departed_flag", "sum"),
        delayed_vehicles=("delayed_departed", "sum"),
        readiness_rate=("readiness_final_flag", "mean"),
        delay_min=("dispatch_delay_min", "mean"),
        soc_real=("soc_salida_pct", "mean"),
        soc_target=("target_soc_salida_pct", "mean"),
    )
    dispatch_base["delay_rate"] = (
        dispatch_base["delayed_vehicles"] / dispatch_base["departed_vehicles"].replace(0, np.nan)
    ).fillna(0.0)
    dispatch_base["fecha"] = pd.to_datetime(dispatch_base["fecha"])

    dispatch_cause = (
        d.loc[d["departed_flag"]]
        .groupby(["turno", "tipo_propulsion", "causa_retraso"], as_index=False)
        .agg(
            delay_min=("dispatch_delay_min", "mean"),
            vehicles=("vehiculo_id", "count"),
        )
        .sort_values("delay_min", ascending=False)
    )

    b = bneck.copy()
    b["fecha"] = pd.to_datetime(b["fecha"], errors="coerce")
    b["severidad"] = np.where(
        b["severidad_media"] >= 4.5,
        "alta",
        np.where(b["severidad_media"] >= 3.0, "media", "baja"),
    )

    b_detail = b.groupby([b["fecha"].dt.date.rename("fecha"), "turno", "area", "severidad"], as_index=False).agg(
        throughput_impact=("impacto_throughput_total", "sum"),
        output_impact=("impacto_salida_total", "sum"),
        stress=("area_stress_score", "mean"),
        eventos=("eventos_cuello", "sum"),
    )
    b_detail["fecha"] = pd.to_datetime(b_detail["fecha"])

    scenarios_view = scenarios[
        [
            "escenario",
            "throughput",
            "espera_carga",
            "ocupacion_pico_patio",
            "riesgo_salida_baja_readiness",
            "estabilidad_operativa",
            "decision_score",
        ]
    ].copy()

    return {
        "throughput": throughput,
        "ev_share_week": ev_share_week,
        "seq_gap": seq_gap,
        "lead_version": lead_version,
        "flow_prop_daily": flow_prop_daily,
        "yard_daily": yard_daily,
        "charge_daily": charge_daily,
        "dispatch_base": dispatch_base,
        "dispatch_cause": dispatch_cause,
        "b_detail": b_detail,
        "priorities": priorities,
        "scenarios": scenarios_view,
        "kpi_readiness": kpi_readiness.copy(),
    }


def _build_payload(meta: dict[str, object], datasets: dict[str, pd.DataFrame]) -> dict[str, object]:
    filters = {
        "turno": sorted(set(datasets["throughput"]["turno"].dropna().astype(str).tolist())),
        "propulsion": sorted(set(datasets["seq_gap"]["tipo_propulsion"].dropna().astype(str).tolist())),
        "version": sorted(set(datasets["lead_version"]["version_id"].dropna().astype(str).tolist())),
        "area": sorted(set(datasets["priorities"]["area"].dropna().astype(str).tolist())),
        "zona_patio": sorted(set(datasets["yard_daily"]["zona_patio"].dropna().astype(str).tolist())),
        "zona_carga": sorted(set(datasets["charge_daily"]["zona_carga"].dropna().astype(str).tolist())),
        "severidad": sorted(set(datasets["b_detail"]["severidad"].dropna().astype(str).tolist())),
    }

    data_serialized = {name: _records(df) for name, df in datasets.items()}

    payload = {
        "meta": meta,
        "filters": filters,
        "data": data_serialized,
    }
    return payload


def _json_for_inline_script(payload: dict[str, object]) -> str:
    """Serializa JSON sin permitir que datos cierren el bloque ``<script>``."""
    return (
        json.dumps(payload, ensure_ascii=False)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def _build_html(payload: dict[str, object], version: str) -> str:
    return render_dashboard_html(serialized_payload=_json_for_inline_script(payload), version=version)


def _write_manifest_and_qc(
    payload: dict[str, object],
    output_path: Path,
    version: str,
    removed: list[str],
) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    datasets = payload["data"]
    row_counts = {k: len(v) for k, v in datasets.items()}
    html = output_path.read_text(encoding="utf-8", errors="ignore")

    priorities = payload["data"].get("priorities", [])
    top_priority_area = "Sin dato"
    if priorities:
        top_priority_area = sorted(
            priorities,
            key=lambda r: float(r.get("operational_priority_index") or 0.0),
            reverse=True,
        )[0].get("area", "Sin dato")

    html_size = output_path.stat().st_size
    canvas_count = html.count("<canvas id=")
    density_limits = {
        "throughput": 1_500,
        "seq_gap": 3_000,
        "yard_daily": 2_500,
        "charge_daily": 4_500,
        "dispatch_base": 3_000,
        "b_detail": 2_000,
    }
    density_guard = all(row_counts.get(k, 0) <= v for k, v in density_limits.items())

    manifest = {
        "dashboard_version": version,
        "official_dashboard": str(output_path.relative_to(PROJECT_ROOT)),
        "html_size_bytes": html_size,
        "html_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        "datasets_rows": row_counts,
        "removed_non_official_dashboards": removed,
        "kpi_validation": payload["meta"].get("kpi_validation", {}),
        "checks": {
            "placeholder_free": all(tok not in html for tok in ["__PAYLOAD__", "__FILTERS__", "__CHARTJS__"]),
            "single_official_dashboard": len(list(output_path.parent.glob("*.html"))) == 1,
            "chart_js_cdn_declared": "cdn.jsdelivr.net/npm/chart.js" in html,
            "kpi_payload_bound": "kpi_official" in html and "const META = PAYLOAD.meta;" in html,
            "html_size_under_6mb": html_size < 6_000_000,
            "canvas_count_expected": canvas_count == 19,
            "severity_filter_wired": "f_severity" in html and "severity:'severidad'" in html,
            "executive_snapshot_consistent": payload["meta"]["executive_snapshot"]["top_area"] == top_priority_area,
            "density_guard": density_guard,
            "kpi_logic_valid": all(payload["meta"].get("kpi_validation", {}).values()),
        },
    }

    write_json_utf8(OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json", manifest)


def run_ev_build_dashboard() -> DashboardResult:
    OUTPUT_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    flow = _read_csv(
        EV_DIR / "vw_vehicle_flow_timeline.csv",
        parse_dates=["fecha_programada", "fecha_real"],
    )
    yard = _read_csv(EV_DIR / "yard_features.csv", parse_dates=["timestamp"])
    charging = _read_csv(EV_DIR / "charging_features.csv", parse_dates=["fecha"])
    dispatch = _read_csv(EV_DIR / "vw_dispatch_readiness.csv", parse_dates=["fecha"])
    bneck = _read_csv(EV_DIR / "vw_shift_bottleneck_summary.csv", parse_dates=["fecha"])
    priorities = _read_csv(EV_DIR / "operational_prioritization_table.csv")
    scenarios = _read_csv(EV_DIR / "scenario_table.csv")
    kpi = _read_csv(EV_DIR / "kpi_operativos.csv")
    kpi_readiness = _read_csv(EV_DIR / "kpi_readiness_shift_version.csv")

    meta = _build_meta(flow, yard, charging, dispatch, priorities, scenarios, kpi)
    datasets = _prepare_datasets(flow, yard, charging, dispatch, bneck, priorities, scenarios, kpi_readiness)
    payload = _build_payload(meta, datasets)

    payload_hash = hashlib.sha1(json.dumps(payload, ensure_ascii=False).encode("utf-8")).hexdigest()[:10]
    version = f"ev-{payload_hash}"

    removed = _remove_non_official_dashboards(OUTPUT_DASHBOARD_DIR, OFFICIAL_DASHBOARD_NAME)

    html = _build_html(payload, version)
    output_path = OUTPUT_DASHBOARD_DIR / OFFICIAL_DASHBOARD_NAME
    output_path.write_text(html, encoding="utf-8")

    _write_manifest_and_qc(payload, output_path, version, removed)

    return DashboardResult(
        path=str(output_path.relative_to(PROJECT_ROOT)),
        version=version,
        payload_size_bytes=len(json.dumps(payload, ensure_ascii=False).encode("utf-8")),
    )


if __name__ == "__main__":
    result = run_ev_build_dashboard()
    print("Panel EV oficial generado")
    print(f"- ruta: {result.path}")
    print(f"- version: {result.version}")
    print(f"- bytes_payload: {result.payload_size_bytes}")
