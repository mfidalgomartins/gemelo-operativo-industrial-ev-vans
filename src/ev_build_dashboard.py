from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import hashlib
import json

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


EV_DIR = DATA_PROCESSED_DIR / "ev_factory"
OFFICIAL_DASHBOARD_NAME = "industrial-ev-operating-command-center.html"


@dataclass
class DashboardResult:
    path: str
    version: str
    payload_size_bytes: int


def _read_csv(path: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Falta dataset para dashboard: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def _records(df: pd.DataFrame) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for row in df.to_dict(orient="records"):
        out: Dict[str, object] = {}
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


def _archive_non_official_dashboards(output_dir: Path, official_name: str) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    legacy_dir = output_dir / "legacy"
    legacy_dir.mkdir(parents=True, exist_ok=True)

    moved: list[str] = []
    for html_file in output_dir.glob("*.html"):
        if html_file.name == official_name:
            continue
        target = legacy_dir / html_file.name
        html_file.replace(target)
        moved.append(str(target.relative_to(PROJECT_ROOT)))
    return moved


def _build_meta(
    flow: pd.DataFrame,
    yard: pd.DataFrame,
    charging: pd.DataFrame,
    priorities: pd.DataFrame,
    scenarios: pd.DataFrame,
    kpi: pd.DataFrame,
) -> Dict[str, object]:
    coverage_min = pd.to_datetime(flow["fecha_real"], errors="coerce").min()
    coverage_max = pd.to_datetime(flow["fecha_real"], errors="coerce").max()

    top_area = priorities.sort_values("operational_priority_index", ascending=False).head(1)
    top_scenario = scenarios.sort_values("decision_score", ascending=False).head(1)

    kpi_row = kpi.iloc[0].to_dict() if not kpi.empty else {}

    return {
        "coverage": f"{coverage_min.date()} a {coverage_max.date()}" if pd.notna(coverage_min) and pd.notna(coverage_max) else "N/A",
        "orders": int(flow["orden_id"].nunique()),
        "vehicles": int(flow["vehiculo_id"].nunique()),
        "yard_zones": int(yard["zona_patio"].nunique()),
        "charge_zones": int(charging["zona_carga"].nunique()),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kpi_official": kpi_row,
        "executive_snapshot": {
            "top_area": str(top_area["area"].iloc[0]) if not top_area.empty else "N/A",
            "top_action": str(top_area["recommended_action"].iloc[0]) if not top_area.empty else "N/A",
            "top_scenario": str(top_scenario["escenario"].iloc[0]) if not top_scenario.empty else "N/A",
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
) -> Dict[str, pd.DataFrame]:
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

    seq_gap = (
        f.groupby([f["fecha_real"].dt.date.rename("fecha"), "turno", "tipo_propulsion"], as_index=False)
        .agg(
            sequence_gap=("planned_to_actual_sequence_gap", "mean"),
            lead_time=("total_internal_lead_time_min", "mean"),
        )
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

    flow_prop_daily = (
        f.groupby([f["fecha_real"].dt.date.rename("fecha"), "turno", "tipo_propulsion"], as_index=False)
        .agg(
            lead_time=("total_internal_lead_time_min", "mean"),
            yard_wait=("yard_wait_time_min", "mean"),
            charge_wait=("charging_wait_time_min", "mean"),
            delay=("dispatch_delay_min", "mean"),
            throughput=("vehiculo_id", "count"),
        )
    )
    flow_prop_daily["fecha"] = pd.to_datetime(flow_prop_daily["fecha"])

    y = yard.copy()
    y["timestamp"] = pd.to_datetime(y["timestamp"], errors="coerce")
    y["fecha"] = y["timestamp"].dt.date

    yard_daily = (
        y.groupby(["fecha", "zona_patio"], as_index=False)
        .agg(
            occupancy=("yard_occupancy_rate", "mean"),
            dwell=("avg_dwell_time", "mean"),
            dwell_p95=("p95_dwell_time", "mean"),
            blocking=("blocking_rate", "mean"),
            non_productive=("non_productive_move_rate", "mean"),
        )
    )
    yard_daily["fecha"] = pd.to_datetime(yard_daily["fecha"]) 

    ch = charging.copy()
    ch["fecha"] = pd.to_datetime(ch["fecha"], errors="coerce")
    ch["slot_utilization"] = np.clip(ch["charger_pressure_score"] / 100, 0, 1.5)

    charge_daily = (
        ch.groupby([ch["fecha"].dt.date.rename("fecha"), "turno", "zona_carga"], as_index=False)
        .agg(
            wait=("avg_wait_to_charge", "mean"),
            utilization=("slot_utilization", "mean"),
            interruption=("interruption_rate", "mean"),
            target_miss=("target_soc_miss_rate", "mean"),
            sessions=("sessions_per_shift", "sum"),
        )
    )
    charge_daily["fecha"] = pd.to_datetime(charge_daily["fecha"]) 

    d = dispatch.copy()
    d["fecha"] = pd.to_datetime(d["fecha"], errors="coerce")

    dispatch_base = (
        d.groupby([d["fecha"].dt.date.rename("fecha"), "turno", "tipo_propulsion"], as_index=False)
        .agg(
            vehicles=("vehiculo_id", "count"),
            delay_rate=("delayed_flag", "mean"),
            readiness_rate=("readiness_final_flag", "mean"),
            delay_min=("dispatch_delay_min", "mean"),
            soc_real=("soc_salida_pct", "mean"),
            soc_target=("target_soc_salida_pct", "mean"),
        )
    )
    dispatch_base["fecha"] = pd.to_datetime(dispatch_base["fecha"])

    dispatch_cause = (
        d.groupby(["turno", "tipo_propulsion", "causa_retraso"], as_index=False)
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

    b_detail = (
        b.groupby([b["fecha"].dt.date.rename("fecha"), "turno", "area", "severidad"], as_index=False)
        .agg(
            throughput_impact=("impacto_throughput_total", "sum"),
            output_impact=("impacto_salida_total", "sum"),
            stress=("area_stress_score", "mean"),
            eventos=("eventos_cuello", "sum"),
        )
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


def _build_payload(meta: Dict[str, object], datasets: Dict[str, pd.DataFrame]) -> Dict[str, object]:
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


def _build_html(payload: Dict[str, object], version: str) -> str:
    return f"""<!doctype html>
<html lang=\"es\">
<head>
<meta charset=\"utf-8\" />
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
<meta name=\"dashboard-version\" content=\"{version}\" />
<title>Gemelo Operativo EV · Dashboard Oficial</title>
<script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js\"></script>
<style>
:root {{
  --bg:#f5f7fb;
  --bg-soft:#ecf2fb;
  --card:#ffffff;
  --line:#d9e1ee;
  --ink:#11243a;
  --muted:#556c86;
  --accent:#0f6d87;
  --ok:#2f7d4d;
  --warn:#c47a1d;
  --danger:#b4374a;
  --shadow:0 10px 24px rgba(16,38,63,.08);
  --control:#ffffff;
  --control-soft:#f0f6ff;
  --grid-x:#e8eef6;
  --grid-y:#e2eaf4;
  --tooltip-bg:#14263a;
  --tooltip-text:#f4f8fd;
  --table-head:#f2f7ff;
  --series-plan:#cf8a2b;
  --series-real:#0a6d8a;
  --series-ev:#267a46;
  --series-gap:#b4374a;
  --series-aux:#5d7f9f;
  --series-yard:#b7791f;
  --series-load:#2f8a87;
  --series-dispatch:#9a5363;
  --series-priority:#4f7b94;
}}
html[data-theme='dark'] {{
  --bg:#0f1724;
  --bg-soft:#152132;
  --card:#172234;
  --line:#2a3a52;
  --ink:#e8eef8;
  --muted:#9eb0c8;
  --accent:#5cb7d4;
  --ok:#63c58c;
  --warn:#f2bc58;
  --danger:#ef7b90;
  --shadow:0 10px 24px rgba(0,0,0,.34);
  --control:#111b2a;
  --control-soft:#1b2940;
  --grid-x:#2a3951;
  --grid-y:#24344d;
  --tooltip-bg:#ecf2ff;
  --tooltip-text:#102238;
  --table-head:#1b2a42;
  --series-plan:#efbc69;
  --series-real:#65bad1;
  --series-ev:#6bc889;
  --series-gap:#ef8899;
  --series-aux:#9bb9d8;
  --series-yard:#f2b85e;
  --series-load:#63ccc6;
  --series-dispatch:#e89cad;
  --series-priority:#8fb8d3;
}}
* {{ box-sizing:border-box; }}
body {{
  margin:0;
  font-family:\"IBM Plex Sans\",\"Segoe UI\",sans-serif;
  color:var(--ink);
  background:radial-gradient(circle at top right,var(--bg-soft),var(--bg));
}}
.wrapper {{ max-width:1600px; margin:0 auto; padding:14px; }}
.section, header, .decision {{ background:var(--card); border:1px solid var(--line); border-radius:14px; box-shadow:var(--shadow); }}
header {{ padding:14px; }}
.head-top {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; flex-wrap:wrap; }}
.title-wrap {{ max-width:1020px; }}
h1 {{ margin:0; font-size:28px; line-height:1.2; }}
.sub {{ margin:6px 0 0 0; font-size:13px; color:var(--muted); line-height:1.4; }}
.actions {{ display:flex; gap:8px; align-items:center; flex-wrap:wrap; }}
.theme-toggle {{
  border:1px solid var(--line);
  background:var(--control-soft);
  color:var(--ink);
  border-radius:10px;
  padding:7px 11px;
  font-size:12px;
  font-weight:700;
  cursor:pointer;
}}
.print-btn {{
  border:1px solid var(--line);
  background:var(--control);
  color:var(--ink);
  border-radius:10px;
  padding:7px 11px;
  font-size:12px;
  font-weight:700;
  cursor:pointer;
}}
.hero-message {{
  margin-top:10px;
  padding:10px 12px;
  border:1px solid var(--line);
  border-left:4px solid var(--accent);
  border-radius:10px;
  background:var(--control);
  font-size:13px;
  line-height:1.45;
}}
.meta {{ margin-top:10px; display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:8px; }}
.pill {{ border:1px solid var(--line); border-radius:10px; padding:8px; font-size:12px; background:var(--control-soft); }}
.pill strong {{ display:block; color:var(--ink); margin-bottom:3px; }}
.filters {{ margin-top:12px; display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:8px; }}
.filters label {{ font-size:11px; color:var(--muted); display:block; margin-bottom:3px; }}
.filters select,.filters input,.filters button {{ width:100%; border:1px solid var(--line); border-radius:8px; padding:8px; font-size:12px; background:var(--control); color:var(--ink); }}
.filters button {{ background:var(--control-soft); font-weight:700; cursor:pointer; }}
.kpis {{ margin-top:12px; display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:8px; }}
.kpi {{ border:1px solid var(--line); border-radius:10px; padding:8px; background:var(--control-soft); min-height:78px; }}
.kpi.kpi-critical {{ border-color:rgba(180,55,74,.35); background:linear-gradient(180deg,var(--control-soft),rgba(180,55,74,.08)); }}
.kpi.kpi-warning {{ border-color:rgba(196,122,29,.35); background:linear-gradient(180deg,var(--control-soft),rgba(196,122,29,.08)); }}
.kpi.kpi-good {{ border-color:rgba(47,125,77,.35); background:linear-gradient(180deg,var(--control-soft),rgba(47,125,77,.08)); }}
.kpi .k {{ font-size:11px; color:var(--muted); line-height:1.3; }}
.kpi .v {{ margin-top:4px; font-size:20px; font-weight:700; line-height:1.2; }}
.kpi .s {{ margin-top:2px; font-size:11px; color:var(--muted); }}
.summary {{ margin-top:10px; display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:10px; }}
.box {{ border:1px solid var(--line); border-radius:10px; padding:10px; background:var(--control-soft); }}
.box h3 {{ margin:0 0 7px 0; font-size:14px; }}
.box ul {{ margin:0; padding-left:16px; font-size:12px; color:var(--ink); line-height:1.4; }}
.box li {{ margin-bottom:5px; }}
.section {{ margin-top:12px; padding:12px; }}
.section[data-section='core'] {{ border-top:4px solid var(--accent); }}
.section[data-section='yard'] {{ border-top:4px solid var(--series-yard); }}
.section[data-section='risk'] {{ border-top:4px solid var(--series-gap); }}
.section-head {{ display:flex; justify-content:space-between; gap:8px; align-items:flex-start; flex-wrap:wrap; }}
.section h2 {{ margin:0; font-size:20px; line-height:1.2; }}
.desc {{ margin:4px 0 0 0; color:var(--muted); font-size:12px; line-height:1.4; max-width:920px; }}
.section-tag {{
  border:1px solid var(--line);
  border-radius:999px;
  padding:4px 10px;
  font-size:11px;
  color:var(--muted);
  background:var(--control-soft);
  font-weight:700;
}}
.grid-2 {{ margin-top:10px; display:grid; grid-template-columns:repeat(auto-fit,minmax(360px,1fr)); gap:10px; align-items:stretch; }}
.grid-3 {{ margin-top:10px; display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:10px; align-items:stretch; }}
.grid-2 > *,.grid-3 > * {{ min-width:0; }}
.chart-card {{ border:1px solid var(--line); border-radius:10px; padding:8px; background:var(--control); min-height:340px; display:flex; flex-direction:column; overflow:hidden; }}
.chart-title {{ font-size:12px; color:var(--ink); margin-bottom:6px; font-weight:600; line-height:1.4; min-height:34px; }}
.canvas-wrap {{ flex:1; min-height:260px; height:300px; overflow:hidden; }}
canvas {{ width:100% !important; height:100% !important; }}
.table-wrap {{ margin-top:10px; border:1px solid var(--line); border-radius:10px; overflow:auto; max-height:420px; }}
.table-tools {{ margin-top:8px; display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px; }}
.table-tools input,.table-tools button {{ border:1px solid var(--line); border-radius:8px; padding:8px; font-size:12px; background:var(--control); color:var(--ink); }}
.table-tools button {{ background:var(--control-soft); cursor:pointer; font-weight:700; }}
table {{ width:100%; border-collapse:collapse; font-size:12px; min-width:920px; }}
th,td {{ padding:8px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; line-height:1.35; word-break:break-word; }}
th {{ position:sticky; top:0; background:var(--table-head); z-index:2; }}
.tier-badge {{
  display:inline-block;
  padding:2px 8px;
  border-radius:999px;
  font-size:11px;
  font-weight:700;
  border:1px solid var(--line);
  background:var(--control-soft);
}}
.tier-badge.t0 {{ border-color:rgba(180,55,74,.4); color:var(--danger); }}
.tier-badge.t1 {{ border-color:rgba(196,122,29,.45); color:var(--warn); }}
.tier-badge.t2 {{ border-color:rgba(47,125,77,.45); color:var(--ok); }}
.decision {{ margin-top:12px; padding:12px; border-left:5px solid var(--accent); }}
.decision h3 {{ margin:0 0 6px 0; font-size:15px; }}
.decision p {{ margin:0; font-size:13px; line-height:1.45; }}
@media (max-width:1300px) {{
  .canvas-wrap {{ height:280px; min-height:240px; }}
}}
@media (max-width:900px) {{
  .wrapper {{ padding:10px; }}
  h1 {{ font-size:24px; }}
  .grid-2,.grid-3 {{ grid-template-columns:1fr; }}
  .chart-card {{ min-height:320px; }}
  .canvas-wrap {{ height:250px; min-height:220px; }}
  table {{ min-width:760px; }}
}}
@media print {{
  @page {{ size: A4 landscape; margin: 10mm; }}
  .filters,.table-tools,.actions {{ display:none !important; }}
  body {{ background:white; color:#10243a; }}
  .wrapper {{ max-width:none; padding:0; }}
  .section,header,.decision {{ box-shadow:none; break-inside: avoid-page; }}
  .section {{ page-break-inside: avoid; margin-top:8px; }}
  .grid-3 {{ grid-template-columns:1fr 1fr 1fr; }}
  .chart-card {{ min-height:260px; }}
  .canvas-wrap {{ min-height:220px; height:220px; }}
  .table-wrap {{ max-height:none; overflow:visible; }}
  table {{ min-width:0; font-size:10px; }}
}}
</style>
</head>
<body>
<div class=\"wrapper\">
<header>
  <div class=\"head-top\">
    <div class=\"title-wrap\">
      <h1>Gemelo Operativo de Lanzamiento Industrial EV</h1>
      <p class=\"sub\">Dashboard ejecutivo para secuenciación, patio, carga y expedición en transición a furgonetas eléctricas.</p>
      <p class=\"sub\"><strong>Cobertura:</strong> <span id=\"meta_coverage\"></span> · <strong>Órdenes:</strong> <span id=\"meta_orders\"></span> · <strong>Vehículos:</strong> <span id=\"meta_vehicles\"></span></p>
    </div>
    <div class=\"actions\">
      <button id=\"theme_toggle\" class=\"theme-toggle\" type=\"button\" aria-label=\"Cambiar tema\">Modo oscuro</button>
      <button id=\"btn_print\" class=\"print-btn\" type=\"button\" aria-label=\"Imprimir dashboard\">Imprimir</button>
    </div>
  </div>
  <div class=\"hero-message\" id=\"hero_message\"></div>
  <div class=\"meta\">
    <div class=\"pill\"><strong>Top Área</strong><span id=\"meta_top_area\"></span></div>
    <div class=\"pill\"><strong>Top Acción</strong><span id=\"meta_top_action\"></span></div>
  </div>

  <div class=\"filters no-print\">
    <div><label>Fecha desde</label><input id=\"f_date_from\" type=\"date\" /></div>
    <div><label>Fecha hasta</label><input id=\"f_date_to\" type=\"date\" /></div>
    <div><label>Turno</label><select id=\"f_turno\"></select></div>
    <div><label>Propulsión</label><select id=\"f_prop\"></select></div>
    <div><label>Versión</label><select id=\"f_version\"></select></div>
    <div><label>Área</label><select id=\"f_area\"></select></div>
    <div><label>Zona Patio</label><select id=\"f_yard\"></select></div>
    <div><label>Zona Carga</label><select id=\"f_charge\"></select></div>
    <div><label>Severidad Cuello</label><select id=\"f_severity\"></select></div>
    <div><label>&nbsp;</label><button id=\"btn_apply\" type=\"button\">Aplicar filtros</button></div>
    <div><label>&nbsp;</label><button id=\"btn_reset\" type=\"button\">Reset filtros</button></div>
  </div>

  <div class=\"kpis\" id=\"kpi_cards\"></div>

  <div class=\"summary\">
    <div class=\"box\">
      <h3>Executive Snapshot</h3>
      <ul id=\"executive_list\"></ul>
    </div>
    <div class=\"box\">
      <h3>Lectura Operativa</h3>
      <ul id=\"operational_list\"></ul>
    </div>
  </div>
</header>

<section class=\"section\" data-section=\"core\">
  <div class=\"section-head\">
    <div>
      <h2>Flujo Global y Secuenciación</h2>
      <p class=\"desc\">Comparación plan-real y evolución del mix EV con densidad controlada para lectura ejecutiva.</p>
    </div>
    <span class=\"section-tag\">Prioridad de flujo</span>
  </div>
  <div class=\"grid-2\">
    <div class=\"chart-card\"><div class=\"chart-title\">Throughput planificado vs real</div><div class=\"canvas-wrap\"><canvas id=\"ch_throughput\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Share EV semanal</div><div class=\"canvas-wrap\"><canvas id=\"ch_ev_share\"></canvas></div></div>
  </div>
  <div class=\"grid-2\">
    <div class=\"chart-card\"><div class=\"chart-title\">Gap de secuencia plan-real</div><div class=\"canvas-wrap\"><canvas id=\"ch_seq_gap\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Lead time medio por versión (top)</div><div class=\"canvas-wrap\"><canvas id=\"ch_lead_version\"></canvas></div></div>
  </div>
</section>

<section class=\"section\" data-section=\"yard\">
  <div class=\"section-head\"><div><h2>Patio y Carga</h2><p class=\"desc\">Lectura de ocupación, dwell, utilización y colas sin sobrecargar ejes ni etiquetas.</p></div><span class=\"section-tag\">Capacidad crítica</span></div>
  <div class=\"grid-2\">
    <div class=\"chart-card\"><div class=\"chart-title\">Ocupación de patio y dwell p95</div><div class=\"canvas-wrap\"><canvas id=\"ch_yard_occ\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Dwell por zona de patio</div><div class=\"canvas-wrap\"><canvas id=\"ch_yard_zone\"></canvas></div></div>
  </div>
  <div class=\"grid-2\">
    <div class=\"chart-card\"><div class=\"chart-title\">Utilización de carga por zona</div><div class=\"canvas-wrap\"><canvas id=\"ch_charge_util\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Cola media de carga</div><div class=\"canvas-wrap\"><canvas id=\"ch_charge_wait\"></canvas></div></div>
  </div>
  <div class=\"grid-2\">
    <div class=\"chart-card\"><div class=\"chart-title\">SOC objetivo vs SOC real</div><div class=\"canvas-wrap\"><canvas id=\"ch_soc\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Interrupciones de carga por zona</div><div class=\"canvas-wrap\"><canvas id=\"ch_interrupt\"></canvas></div></div>
  </div>
</section>

<section class=\"section\" data-section=\"risk\">
  <div class=\"section-head\"><div><h2>Expedición, Cuellos y Priorización</h2><p class=\"desc\">Señales de riesgo de salida y ranking de acciones para intervención.</p></div><span class=\"section-tag\">Decisión ejecutiva</span></div>
  <div class=\"grid-2\">
    <div class=\"chart-card\"><div class=\"chart-title\">Retraso medio por causa</div><div class=\"canvas-wrap\"><canvas id=\"ch_delay_cause\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Delay rate y readiness por turno</div><div class=\"canvas-wrap\"><canvas id=\"ch_shift_readiness\"></canvas></div></div>
  </div>
  <div class=\"grid-3\">
    <div class=\"chart-card\"><div class=\"chart-title\">Impacto de cuellos por área</div><div class=\"canvas-wrap\"><canvas id=\"ch_bneck_area\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Matriz de riesgo por área</div><div class=\"canvas-wrap\"><canvas id=\"ch_risk_matrix\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Ranking de acciones recomendadas</div><div class=\"canvas-wrap\"><canvas id=\"ch_actions\"></canvas></div></div>
  </div>
  <div class=\"grid-2\">
    <div class=\"chart-card\"><div class=\"chart-title\">Comparación EV vs ICE</div><div class=\"canvas-wrap\"><canvas id=\"ch_ev_vs_ice\"></canvas></div></div>
    <div class=\"chart-card\"><div class=\"chart-title\">Escenarios (comparador oficial)</div>
      <div style=\"display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:6px;\">
        <label style=\"font-size:12px;color:var(--muted);\">Escenario</label>
        <select id=\"scenario_select\" style=\"border:1px solid var(--line);border-radius:8px;padding:6px;font-size:12px;\"></select>
      </div>
      <div class=\"canvas-wrap\"><canvas id=\"ch_scenarios\"></canvas></div>
    </div>
  </div>

  <div class=\"table-tools no-print\">
    <div style=\"display:flex;gap:8px;align-items:center;flex-wrap:wrap;\">
      <input id=\"table_search\" type=\"text\" placeholder=\"Buscar área, driver, acción...\" />
      <button id=\"btn_export\" type=\"button\">Export CSV filtrado</button>
    </div>
    <div id=\"table_status\" style=\"font-size:12px;color:var(--muted);\"></div>
  </div>
  <div class=\"table-wrap\">
    <table id=\"priority_table\">
      <thead>
        <tr>
          <th>Área</th>
          <th>OPI</th>
          <th>Tier</th>
          <th>Main Driver</th>
          <th>Action</th>
          <th>Bottleneck</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</section>

<div class=\"decision\">
  <h3>Decisión Ejecutiva Recomendada</h3>
  <p id=\"decision_text\"></p>
</div>
</div>

<script>
const PAYLOAD = {json.dumps(payload, ensure_ascii=False)};
const META = PAYLOAD.meta;
const FILTERS = PAYLOAD.filters;
const DATA = PAYLOAD.data;
const state = {{ sortCol: 'operational_priority_index', sortAsc: false, scenario: '' }};
let tableRows = [];
const charts = {{}};
const THEME_KEY = 'ev_dashboard_theme';

function n(v) {{ const x = Number(v); return Number.isFinite(x) ? x : 0; }}
function pct(v) {{ return (n(v)*100).toFixed(1) + '%'; }}
function dstr(v) {{
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return '';
  return d.toISOString().slice(0,10);
}}
function cssVar(name) {{ return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }}

function themeColors() {{
  return {{
    ink: cssVar('--ink'),
    muted: cssVar('--muted'),
    gridX: cssVar('--grid-x'),
    gridY: cssVar('--grid-y'),
    tooltipBg: cssVar('--tooltip-bg'),
    tooltipText: cssVar('--tooltip-text'),
    plan: cssVar('--series-plan'),
    real: cssVar('--series-real'),
    ev: cssVar('--series-ev'),
    gap: cssVar('--series-gap'),
    aux: cssVar('--series-aux'),
    yard: cssVar('--series-yard'),
    load: cssVar('--series-load'),
    dispatch: cssVar('--series-dispatch'),
    priority: cssVar('--series-priority'),
    warn: cssVar('--warn'),
  }};
}}

function hexToRgba(hex, alpha) {{
  const h = (hex || '').replace('#', '').trim();
  if (h.length !== 6) return hex;
  const r = parseInt(h.slice(0,2), 16);
  const g = parseInt(h.slice(2,4), 16);
  const b = parseInt(h.slice(4,6), 16);
  return 'rgba(' + r + ',' + g + ',' + b + ',' + alpha + ')';
}}

function setMeta() {{
  document.getElementById('meta_coverage').textContent = META.coverage || 'N/A';
  document.getElementById('meta_orders').textContent = (META.orders || 0).toLocaleString('es-ES');
  document.getElementById('meta_vehicles').textContent = (META.vehicles || 0).toLocaleString('es-ES');
  document.getElementById('meta_top_area').textContent = META.executive_snapshot.top_area || 'N/A';
  document.getElementById('meta_top_action').textContent = META.executive_snapshot.top_action || 'N/A';

  const list = [
    'Top área crítica: ' + (META.executive_snapshot.top_area || 'N/A'),
    'Acción recomendada: ' + (META.executive_snapshot.top_action || 'N/A'),
    'Escenario más robusto: ' + (META.executive_snapshot.top_scenario || 'N/A'),
    'Riesgo dominante de salida: retraso por readiness en picos EV.'
  ];
  document.getElementById('executive_list').innerHTML = list.map(x => '<li>' + x + '</li>').join('');

  const opList = [
    'Priorizar zonas con mayor dwell p95 y blocking recurrente.',
    'Reservar capacidad de carga para unidades con salida inmediata.',
    'Evitar acumulación en patio en turnos de alta presión logística.'
  ];
  document.getElementById('operational_list').innerHTML = opList.map(x => '<li>' + x + '</li>').join('');

  document.getElementById('hero_message').textContent =
    'Foco inmediato: ' + (META.executive_snapshot.top_area || 'N/A')
    + ' · Acción líder: ' + (META.executive_snapshot.top_action || 'N/A')
    + ' · Escenario recomendado: ' + (META.executive_snapshot.top_scenario || 'N/A') + '.';
}}

function fillSelect(id, values, label) {{
  const el = document.getElementById(id);
  el.innerHTML = '';
  const all = document.createElement('option');
  all.value = 'ALL';
  all.textContent = 'Todos (' + label + ')';
  el.appendChild(all);
  (values || []).forEach(v => {{
    const o = document.createElement('option');
    o.value = String(v);
    o.textContent = String(v);
    el.appendChild(o);
  }});
}}

function setupFilters() {{
  fillSelect('f_turno', FILTERS.turno, 'turno');
  fillSelect('f_prop', FILTERS.propulsion, 'propulsión');
  fillSelect('f_version', FILTERS.version, 'versión');
  fillSelect('f_area', FILTERS.area, 'área');
  fillSelect('f_yard', FILTERS.zona_patio, 'zona patio');
  fillSelect('f_charge', FILTERS.zona_carga, 'zona carga');
  fillSelect('f_severity', FILTERS.severidad, 'severidad');

  const th = DATA.throughput;
  if (th.length) {{
    const dates = th.map(r => dstr(r.fecha)).sort();
    document.getElementById('f_date_from').value = dates[0];
    document.getElementById('f_date_to').value = dates[dates.length - 1];
    document.getElementById('f_date_from').min = dates[0];
    document.getElementById('f_date_from').max = dates[dates.length - 1];
    document.getElementById('f_date_to').min = dates[0];
    document.getElementById('f_date_to').max = dates[dates.length - 1];
  }}

  const scen = document.getElementById('scenario_select');
  DATA.scenarios.forEach((s, i) => {{
    const op = document.createElement('option');
    op.value = s.escenario;
    op.textContent = s.escenario;
    scen.appendChild(op);
    if (!state.scenario && i === 0) state.scenario = s.escenario;
  }});
  scen.value = state.scenario;
}}

function getFilterState() {{
  let from = document.getElementById('f_date_from').value;
  let to = document.getElementById('f_date_to').value;
  if (from && to && from > to) {{
    const tmp = from;
    from = to;
    to = tmp;
    document.getElementById('f_date_from').value = from;
    document.getElementById('f_date_to').value = to;
  }}
  return {{
    from: from,
    to: to,
    turno: document.getElementById('f_turno').value,
    prop: document.getElementById('f_prop').value,
    version: document.getElementById('f_version').value,
    area: document.getElementById('f_area').value,
    yard: document.getElementById('f_yard').value,
    charge: document.getElementById('f_charge').value,
    severity: document.getElementById('f_severity').value,
  }};
}}

function inDateRange(d, from, to) {{
  const x = dstr(d);
  return (!from || x >= from) && (!to || x <= to);
}}

function filterRows(rows, map) {{
  const f = getFilterState();
  return rows.filter(r => {{
    if (map.date && !inDateRange(r[map.date], f.from, f.to)) return false;
    if (map.turno && f.turno !== 'ALL' && String(r[map.turno]) !== f.turno) return false;
    if (map.prop && f.prop !== 'ALL' && String(r[map.prop]) !== f.prop) return false;
    if (map.version && f.version !== 'ALL' && String(r[map.version]) !== f.version) return false;
    if (map.area && f.area !== 'ALL' && String(r[map.area]) !== f.area) return false;
    if (map.yard && f.yard !== 'ALL' && String(r[map.yard]) !== f.yard) return false;
    if (map.charge && f.charge !== 'ALL' && String(r[map.charge]) !== f.charge) return false;
    if (map.severity && f.severity !== 'ALL' && String(r[map.severity]) !== f.severity) return false;
    return true;
  }});
}}

function groupMean(rows, key, value) {{
  const m = new Map();
  rows.forEach(r => {{
    const k = String(r[key]);
    const v = n(r[value]);
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(v);
  }});
  const labels = Array.from(m.keys()).sort();
  const vals = labels.map(l => m.get(l).reduce((a,b) => a+b, 0) / m.get(l).length);
  return {{ labels, vals }};
}}

function aggregateBy(rows, key, specs) {{
  const m = new Map();
  rows.forEach(r => {{
    const k = String(r[key] ?? 'N/A');
    if (!m.has(k)) {{
      const seed = {{}};
      specs.forEach(s => {{ seed[s.name] = {{ sum: 0, count: 0 }}; }});
      m.set(k, seed);
    }}
    const bucket = m.get(k);
    specs.forEach(s => {{
      bucket[s.name].sum += n(r[s.src]);
      bucket[s.name].count += 1;
    }});
  }});
  return Array.from(m.entries()).map(([k, v]) => {{
    const out = {{ [key]: k }};
    specs.forEach(s => {{
      out[s.name] = s.mode === 'sum'
        ? v[s.name].sum
        : (v[s.name].count ? v[s.name].sum / v[s.name].count : 0);
    }});
    return out;
  }});
}}

function downsample(labels, seriesList, maxPoints = 50) {{
  if (labels.length <= maxPoints) return {{ labels, seriesList }};
  const step = Math.ceil(labels.length / maxPoints);
  const idx = [];
  for (let i=0;i<labels.length;i+=step) idx.push(i);
  return {{
    labels: idx.map(i => labels[i]),
    seriesList: seriesList.map(s => idx.map(i => s[i]))
  }};
}}

function truncLabels(arr, n=20) {{
  return arr.map(x => x.length > n ? x.slice(0, n-1) + '…' : x);
}}

function renderOfficialKpis() {{
  const k = META.kpi_official || {{}};
  const cards = [
    ['Throughput real', Math.round(n(k.throughput_real || 0)), 'Ejecución actual', 'kpi-good'],
    ['Gap vs plan', Math.round(n(k.throughput_gap || 0)), 'Desviación neta', n(k.throughput_gap || 0) < 0 ? 'kpi-critical' : 'kpi-warning'],
    ['Share EV', pct(k.share_ev || 0), 'Mix de fabricación', 'kpi-warning'],
    ['Ocupación pico patio', pct(k.ocupacion_pico_patio || 0), 'Riesgo de saturación', n(k.ocupacion_pico_patio || 0) > 0.85 ? 'kpi-critical' : 'kpi-warning'],
    ['Utilización carga', pct(k.utilizacion_media_cargadores || 0), 'Capacidad usada', n(k.utilizacion_media_cargadores || 0) > 0.82 ? 'kpi-critical' : 'kpi-warning'],
    ['Delay rate salida', pct(k.ratio_salida_retrasada || 0), 'Impacto expedición', n(k.ratio_salida_retrasada || 0) > 0.12 ? 'kpi-critical' : 'kpi-warning'],
    ['Vehículos no ready', Math.round(n(k.vehiculos_no_ready || 0)), 'Backlog de salida', n(k.vehiculos_no_ready || 0) > 0 ? 'kpi-critical' : 'kpi-good'],
    ['Tiempo medio patio', n(k.tiempo_medio_patio_min || 0).toFixed(1) + ' min', 'Espera interna', 'kpi-warning'],
    ['Dwell p95', n(k.dwell_p95_min || 0).toFixed(1) + ' min', 'Cola extrema', 'kpi-warning'],
    ['Áreas críticas', Math.round(n(k.areas_criticas || 0)), 'Foco operativo', n(k.areas_criticas || 0) > 0 ? 'kpi-critical' : 'kpi-good'],
    ['Readiness global', n(k.score_readiness_global || 0).toFixed(1), 'Preparación salida', n(k.score_readiness_global || 0) >= 70 ? 'kpi-good' : 'kpi-warning'],
    ['Causa cuello', (k.causa_principal_cuello || 'N/A'), 'Driver principal', 'kpi-warning'],
  ];
  document.getElementById('kpi_cards').innerHTML = cards.map(c =>
    '<div class="kpi ' + c[3] + '"><div class="k">' + c[0] + '</div><div class="v">' + c[1] + '</div><div class="s">' + c[2] + '</div></div>'
  ).join('');
}}

function applyTheme(theme) {{
  document.documentElement.setAttribute('data-theme', theme);
  try {{ localStorage.setItem(THEME_KEY, theme); }} catch (e) {{}}
  const btn = document.getElementById('theme_toggle');
  if (btn) btn.textContent = theme === 'dark' ? 'Modo claro' : 'Modo oscuro';
  applyChartTheme();
}}

function initTheme() {{
  let stored = null;
  try {{ stored = localStorage.getItem(THEME_KEY); }} catch (e) {{ stored = null; }}
  const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  const theme = stored || (prefersDark ? 'dark' : 'light');
  applyTheme(theme);
  const btn = document.getElementById('theme_toggle');
  if (btn) {{
    btn.addEventListener('click', () => {{
      const current = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
      applyTheme(current === 'dark' ? 'light' : 'dark');
      updateCharts();
    }});
  }}
}}

function makeChart(id, type, extra={{}}) {{
  const c = themeColors();
  charts[id] = new Chart(document.getElementById(id), {{
    type,
    data: {{ labels: [], datasets: [] }},
    options: Object.assign({{
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: {{ mode: 'index', intersect: false }},
      layout: {{ padding: {{ top: 4, right: 8, bottom: 2, left: 4 }} }},
      plugins: {{
        legend: {{
          position: 'bottom',
          maxHeight: 48,
          labels: {{
            boxWidth: 10,
            usePointStyle: true,
            pointStyle: 'rectRounded',
            padding: 12,
            font: {{ size: 11 }},
            color: c.ink,
          }},
        }},
        tooltip: {{
          titleFont: {{ size: 12 }},
          bodyFont: {{ size: 11 }},
          backgroundColor: c.tooltipBg,
          titleColor: c.tooltipText,
          bodyColor: c.tooltipText,
        }}
      }},
      scales: {{
        x: {{
          ticks: {{ autoSkip: true, maxTicksLimit: 8, maxRotation: 0, minRotation: 0, font: {{ size: 10 }}, color: c.muted }},
          grid: {{ color: c.gridX }}
        }},
        y: {{
          ticks: {{ font: {{ size: 10 }}, color: c.muted }},
          grid: {{ color: c.gridY }}
        }}
      }}
    }}, extra)
  }});
}}

function applyChartTheme() {{
  const c = themeColors();
  Chart.defaults.color = c.ink;
  Object.values(charts).forEach(chart => {{
    if (!chart?.options) return;
    if (chart.options.plugins?.legend?.labels) {{
      chart.options.plugins.legend.labels.color = c.ink;
    }}
    if (chart.options.plugins?.tooltip) {{
      chart.options.plugins.tooltip.backgroundColor = c.tooltipBg;
      chart.options.plugins.tooltip.titleColor = c.tooltipText;
      chart.options.plugins.tooltip.bodyColor = c.tooltipText;
    }}
    if (chart.options.scales) {{
      Object.keys(chart.options.scales).forEach(axis => {{
        const cfg = chart.options.scales[axis];
        if (!cfg) return;
        if (!cfg.ticks) cfg.ticks = {{}};
        cfg.ticks.color = c.muted;
        if (!cfg.grid) cfg.grid = {{}};
        if (cfg.grid.drawOnChartArea !== false) {{
          cfg.grid.color = axis === 'x' ? c.gridX : c.gridY;
        }}
      }});
    }}
    chart.update('none');
  }});
}}

function updateCharts() {{
  const c = themeColors();
  const fThrough = filterRows(DATA.throughput, {{ date:'fecha', turno:'turno' }});
  const fSeq = filterRows(DATA.seq_gap, {{ date:'fecha', turno:'turno', prop:'tipo_propulsion' }});
  const fLead = filterRows(DATA.lead_version, {{ prop:'tipo_propulsion', version:'version_id' }});
  const fYard = filterRows(DATA.yard_daily, {{ date:'fecha', yard:'zona_patio' }});
  const fCharge = filterRows(DATA.charge_daily, {{ date:'fecha', turno:'turno', charge:'zona_carga' }});
  const fDispatchBase = filterRows(DATA.dispatch_base, {{ date:'fecha', turno:'turno', prop:'tipo_propulsion' }});
  const fDispatchCause = filterRows(DATA.dispatch_cause, {{ turno:'turno', prop:'tipo_propulsion' }});
  const fBDetail = filterRows(DATA.b_detail, {{ date:'fecha', turno:'turno', area:'area', severity:'severidad' }});
  const fFlowProp = filterRows(DATA.flow_prop_daily, {{ date:'fecha', turno:'turno' }});
  const fPrio = filterRows(DATA.priorities, {{ area:'area' }});
  const fRisk = fPrio.map(r => ({{
    area: r.area,
    throughput_loss_score: r.throughput_loss_score,
    dispatch_risk_score: r.dispatch_risk_score,
    operational_priority_index: r.operational_priority_index,
  }}));

  // Throughput
  const tPlan = groupMean(fThrough, 'fecha', 'throughput_plan');
  const tReal = groupMean(fThrough, 'fecha', 'throughput_real');
  const dsT = downsample(tPlan.labels, [tPlan.vals, tReal.vals], 50);
  charts.ch_throughput.data.labels = dsT.labels;
  charts.ch_throughput.data.datasets = [
    {{ label:'Plan', data:dsT.seriesList[0], borderColor:c.plan, borderDash:[6,4], tension:.25, pointRadius:0 }},
    {{ label:'Real', data:dsT.seriesList[1], borderColor:c.real, backgroundColor:hexToRgba(c.real, .14), fill:true, tension:.25, pointRadius:0 }}
  ];
  charts.ch_throughput.update();

  // EV share weekly
  charts.ch_ev_share.data.labels = DATA.ev_share_week.map(r => dstr(r.week));
  charts.ch_ev_share.data.datasets = [
    {{ label:'Share EV', data:DATA.ev_share_week.map(r => n(r.share_ev)*100), borderColor:c.ev, backgroundColor:hexToRgba(c.ev, .15), fill:true, tension:.25, pointRadius:0 }}
  ];
  charts.ch_ev_share.update();

  // Sequence gap
  const sg = groupMean(fSeq, 'fecha', 'sequence_gap');
  const dsSG = downsample(sg.labels, [sg.vals], 50);
  charts.ch_seq_gap.data.labels = dsSG.labels;
  charts.ch_seq_gap.data.datasets = [
    {{ label:'Gap secuencia', data:dsSG.seriesList[0], borderColor:c.gap, tension:.25, pointRadius:0 }},
    {{ label:'Objetivo', data:dsSG.seriesList[0].map(() => 0), borderColor:c.aux, borderDash:[4,4], pointRadius:0 }}
  ];
  charts.ch_seq_gap.update();

  // Lead by version
  const leadTop = [...fLead].sort((a,b) => n(b.lead_time)-n(a.lead_time)).slice(0,12);
  charts.ch_lead_version.data.labels = truncLabels(leadTop.map(r => r.version_id), 20);
  charts.ch_lead_version.data.datasets = [
    {{ label:'Lead time (min)', data:leadTop.map(r => n(r.lead_time)), backgroundColor:c.priority }}
  ];
  charts.ch_lead_version.update();

  // Yard occ
  const yOcc = groupMean(fYard, 'fecha', 'occupancy');
  const yP95 = groupMean(fYard, 'fecha', 'dwell_p95');
  const dsY = downsample(yOcc.labels, [yOcc.vals.map(v => v*100), yP95.vals], 50);
  charts.ch_yard_occ.options.scales = {{
    x: {{
      ticks: {{ autoSkip:true, maxTicksLimit:10, font:{{size:10}}, color:c.muted }},
      grid: {{ color:c.gridX }}
    }},
    y: {{
      beginAtZero:true,
      title:{{display:true,text:'Ocupación %', color:c.muted}},
      ticks: {{ color:c.muted }},
      grid: {{ color:c.gridY }}
    }},
    y1: {{
      beginAtZero:true,
      position:'right',
      title:{{display:true,text:'Dwell p95 (min)', color:c.muted}},
      ticks: {{ color:c.muted }},
      grid:{{drawOnChartArea:false}}
    }}
  }};
  charts.ch_yard_occ.data.labels = dsY.labels;
  charts.ch_yard_occ.data.datasets = [
    {{ label:'Ocupación %', data:dsY.seriesList[0], borderColor:c.yard, tension:.2, yAxisID:'y', pointRadius:0 }},
    {{ label:'Dwell p95', data:dsY.seriesList[1], borderColor:c.priority, tension:.2, yAxisID:'y1', pointRadius:0 }}
  ];
  charts.ch_yard_occ.update();

  const yardZone = aggregateBy(fYard, 'zona_patio', [
    {{ name:'dwell', src:'dwell', mode:'avg' }},
    {{ name:'blocking', src:'blocking', mode:'avg' }},
  ]).sort((a,b) => n(b.dwell) - n(a.dwell));
  charts.ch_yard_zone.data.labels = truncLabels(yardZone.map(r => r.zona_patio), 20);
  charts.ch_yard_zone.data.datasets = [
    {{ label:'Dwell medio', data:yardZone.map(r => n(r.dwell)), backgroundColor:c.priority }},
    {{ label:'Blocking %', data:yardZone.map(r => n(r.blocking)*100), backgroundColor:c.gap }}
  ];
  charts.ch_yard_zone.update();

  const chargeZone = aggregateBy(fCharge, 'zona_carga', [
    {{ name:'utilization', src:'utilization', mode:'avg' }},
    {{ name:'target_miss', src:'target_miss', mode:'avg' }},
    {{ name:'interruption', src:'interruption', mode:'avg' }},
  ]).sort((a,b) => n(b.utilization) - n(a.utilization));
  charts.ch_charge_util.data.labels = truncLabels(chargeZone.map(r => r.zona_carga), 18);
  charts.ch_charge_util.data.datasets = [
    {{ label:'Utilización %', data:chargeZone.map(r => n(r.utilization)*100), backgroundColor:c.load }},
    {{ label:'Target miss %', data:chargeZone.map(r => n(r.target_miss)*100), backgroundColor:c.warn }}
  ];
  charts.ch_charge_util.update();

  const cw = groupMean(fCharge, 'fecha', 'wait');
  const dsCW = downsample(cw.labels, [cw.vals], 50);
  charts.ch_charge_wait.data.labels = dsCW.labels;
  charts.ch_charge_wait.data.datasets = [{{ label:'Espera carga (min)', data:dsCW.seriesList[0], borderColor:c.yard, tension:.2, pointRadius:0 }}];
  charts.ch_charge_wait.update();

  const soc = groupMean(fDispatchBase, 'fecha', 'soc_real');
  const tar = groupMean(fDispatchBase, 'fecha', 'soc_target');
  const dsSOC = downsample(soc.labels, [soc.vals, tar.vals], 50);
  charts.ch_soc.data.labels = dsSOC.labels;
  charts.ch_soc.data.datasets = [
    {{ label:'SOC real', data:dsSOC.seriesList[0], borderColor:c.ev, tension:.2, pointRadius:0 }},
    {{ label:'SOC objetivo', data:dsSOC.seriesList[1], borderColor:c.aux, borderDash:[5,4], tension:.2, pointRadius:0 }}
  ];
  charts.ch_soc.update();

  charts.ch_interrupt.data.labels = truncLabels(chargeZone.map(r => r.zona_carga), 18);
  charts.ch_interrupt.data.datasets = [{{ label:'Interrupción %', data:chargeZone.map(r => n(r.interruption)*100), backgroundColor:c.dispatch }}];
  charts.ch_interrupt.update();

  const causeTop = [...fDispatchCause].reduce((acc, r) => {{
    const k = r.causa_retraso || 'SIN_DATO';
    if (!acc[k]) acc[k] = {{ delay:0, w:0 }};
    const w = Math.max(1, n(r.vehicles));
    acc[k].delay += n(r.delay_min)*w;
    acc[k].w += w;
    return acc;
  }}, {{}});
  const causeArr = Object.keys(causeTop).map(k => ({{ cause:k, delay:causeTop[k].w ? causeTop[k].delay/causeTop[k].w : 0 }})).sort((a,b)=>b.delay-a.delay).slice(0,10);
  charts.ch_delay_cause.data.labels = truncLabels(causeArr.map(x => x.cause), 24);
  charts.ch_delay_cause.data.datasets = [{{ label:'Retraso medio (min)', data:causeArr.map(x => x.delay), backgroundColor:c.dispatch }}];
  charts.ch_delay_cause.update();

  charts.ch_shift_readiness.data.labels = fDispatchBase.length ? [...new Set(fDispatchBase.map(r => r.turno))] : [...new Set(DATA.dispatch_base.map(r => r.turno))];
  const shiftRows = charts.ch_shift_readiness.data.labels.map(t => {{
    const rows = fDispatchBase.filter(r => String(r.turno) === String(t));
    if (!rows.length) return {{ d:0, r:0 }};
    const w = rows.reduce((a,r)=>a+Math.max(1,n(r.vehicles)),0);
    return {{
      d: rows.reduce((a,r)=>a+n(r.delay_rate)*Math.max(1,n(r.vehicles)),0) / w,
      r: rows.reduce((a,r)=>a+n(r.readiness_rate)*Math.max(1,n(r.vehicles)),0) / w,
    }};
  }});
  charts.ch_shift_readiness.data.datasets = [
    {{ label:'Delay rate %', data:shiftRows.map(x => x.d*100), backgroundColor:c.gap }},
    {{ label:'Readiness rate %', data:shiftRows.map(x => x.r*100), backgroundColor:c.ev }}
  ];
  charts.ch_shift_readiness.update();

  const bArea = aggregateBy(fBDetail, 'area', [
    {{ name:'throughput_impact', src:'throughput_impact', mode:'sum' }},
    {{ name:'output_impact', src:'output_impact', mode:'sum' }},
  ]).sort((a,b) => n(b.throughput_impact) - n(a.throughput_impact));
  charts.ch_bneck_area.data.labels = truncLabels(bArea.map(r => r.area), 18);
  charts.ch_bneck_area.data.datasets = [
    {{ label:'Impacto throughput', data:bArea.map(r => n(r.throughput_impact)), backgroundColor:c.priority }},
    {{ label:'Impacto salida', data:bArea.map(r => n(r.output_impact)), backgroundColor:c.yard }}
  ];
  charts.ch_bneck_area.update();

  charts.ch_risk_matrix.data.datasets = [{{
    label:'Áreas',
    data:fRisk.map(r => ({{ x:n(r.throughput_loss_score), y:n(r.dispatch_risk_score), r:6+n(r.operational_priority_index)/12, area:r.area }})),
    backgroundColor:hexToRgba(c.gap, .42),
    borderColor:c.gap
  }}];
  charts.ch_risk_matrix.options.plugins.tooltip = {{ callbacks: {{ label: (ctx) => {{ const p = ctx.raw || {{}}; return (p.area || 'Área') + ' · TLoss ' + n(p.x).toFixed(1) + ' · DRisk ' + n(p.y).toFixed(1); }} }} }};
  charts.ch_risk_matrix.update();

  const act = aggregateBy(fPrio, 'recommended_action', [
    {{ name:'priority_index', src:'operational_priority_index', mode:'avg' }},
  ]).sort((a,b)=>n(b.priority_index)-n(a.priority_index)).slice(0,10);
  charts.ch_actions.data.labels = truncLabels(act.map(r => r.recommended_action), 28);
  charts.ch_actions.data.datasets = [{{ label:'OPI medio', data:act.map(r => n(r.priority_index)), backgroundColor:c.priority }}];
  charts.ch_actions.update();

  charts.ch_ev_vs_ice.data.labels = ['Lead time','Yard wait','Charge wait','Delay'];
  const evIce = aggregateBy(fFlowProp, 'tipo_propulsion', [
    {{ name:'lead_time', src:'lead_time', mode:'avg' }},
    {{ name:'yard_wait', src:'yard_wait', mode:'avg' }},
    {{ name:'charge_wait', src:'charge_wait', mode:'avg' }},
    {{ name:'delay', src:'delay', mode:'avg' }},
  ]);
  const rowsEV = evIce.find(r => String(r.tipo_propulsion) === 'EV') || {{}};
  const rowsICE = evIce.find(r => String(r.tipo_propulsion) === 'ICE') || {{}};
  charts.ch_ev_vs_ice.data.datasets = [
    {{ label:'EV', data:[n(rowsEV.lead_time), n(rowsEV.yard_wait), n(rowsEV.charge_wait), n(rowsEV.delay)], backgroundColor:c.real }},
    {{ label:'ICE', data:[n(rowsICE.lead_time), n(rowsICE.yard_wait), n(rowsICE.charge_wait), n(rowsICE.delay)], backgroundColor:c.dispatch }}
  ];
  charts.ch_ev_vs_ice.update();

  const scenarioRows = DATA.scenarios;
  charts.ch_scenarios.data.labels = truncLabels(scenarioRows.map(r => r.escenario), 22);
  charts.ch_scenarios.data.datasets = [
    {{ label:'Decision Score', data:scenarioRows.map(r => n(r.decision_score)), backgroundColor:c.priority }},
    {{ label:'Estabilidad', data:scenarioRows.map(r => n(r.estabilidad_operativa)), backgroundColor:c.ev }}
  ];
  charts.ch_scenarios.update();

  renderPriorityTable(fPrio);
}}

function renderPriorityTable(rows) {{
  let data = [...rows];
  const q = document.getElementById('table_search').value.toLowerCase().trim();
  if (q) data = data.filter(r => JSON.stringify(r).toLowerCase().includes(q));
  data.sort((a,b) => n(b.operational_priority_index) - n(a.operational_priority_index));
  tableRows = data;

  const body = document.querySelector('#priority_table tbody');
  const tierClass = (tier) => {{
    const t = String(tier || '').toLowerCase();
    if (t.includes('intervenir')) return 't0';
    if (t.includes('estabilizar') || t.includes('monitorizar')) return 't1';
    return 't2';
  }};
  body.innerHTML = data.map(r =>
    '<tr>' +
    '<td>' + (r.area || 'N/A') + '</td>' +
    '<td>' + n(r.operational_priority_index).toFixed(1) + '</td>' +
    '<td><span class="tier-badge ' + tierClass(r.area_priority_tier) + '">' + (r.area_priority_tier || 'N/A') + '</span></td>' +
    '<td>' + (r.main_risk_driver || 'N/A') + '</td>' +
    '<td>' + (r.recommended_action || 'N/A') + '</td>' +
    '<td>' + (r.main_bottleneck_driver || 'N/A') + '</td>' +
    '</tr>'
  ).join('');

  document.getElementById('table_status').textContent = 'Filas visibles: ' + data.length.toLocaleString('es-ES');

  const top = data[0];
  if (top) {{
    document.getElementById('decision_text').textContent =
      'Intervenir primero en ' + top.area + ' (OPI ' + n(top.operational_priority_index).toFixed(1) + '). '
      + 'Acción prioritaria: ' + (top.recommended_action || 'N/A') + '. '
      + 'Driver dominante: ' + (top.main_risk_driver || 'N/A') + '. '
      + 'Escenario recomendado: ' + (state.scenario || 'N/A') + '.';
  }} else {{
    document.getElementById('decision_text').textContent = 'No hay filas con los filtros actuales.';
  }}
}}

function exportCsv() {{
  if (!tableRows.length) return;
  const cols = ['area','operational_priority_index','area_priority_tier','main_risk_driver','recommended_action','main_bottleneck_driver'];
  const lines = [cols.join(',')];
  tableRows.forEach(r => {{
    lines.push(cols.map(c => '"' + String(r[c] ?? '').replace(/"/g, '""') + '"').join(','));
  }});
  const blob = new Blob([lines.join('\\n')], {{ type: 'text/csv;charset=utf-8;' }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'dashboard_prioridades_filtradas.csv';
  a.click();
  URL.revokeObjectURL(url);
}}

function bind() {{
  const filterIds = ['f_date_from','f_date_to','f_turno','f_prop','f_version','f_area','f_yard','f_charge','f_severity'];
  filterIds.forEach(id => {{
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('change', updateCharts);
    el.addEventListener('input', updateCharts);
  }});
  document.getElementById('btn_apply').addEventListener('click', updateCharts);
  document.getElementById('btn_reset').addEventListener('click', () => {{
    setupFilters();
    document.getElementById('table_search').value = '';
    updateCharts();
  }});
  document.getElementById('table_search').addEventListener('input', () => renderPriorityTable(filterRows(DATA.priorities, {{ area:'area' }})));
  document.getElementById('btn_export').addEventListener('click', exportCsv);
  document.getElementById('scenario_select').addEventListener('change', (e) => {{ state.scenario = e.target.value; updateCharts(); }});
  document.getElementById('btn_print').addEventListener('click', () => window.print());
}}

function initCharts() {{
  const c = themeColors();
  Chart.defaults.font.family = 'IBM Plex Sans, Segoe UI, sans-serif';
  Chart.defaults.color = c.ink;
  makeChart('ch_throughput', 'line', {{ elements: {{ line: {{ borderWidth: 2 }}, point: {{ radius: 0, hoverRadius: 3 }} }} }});
  makeChart('ch_ev_share', 'line', {{ elements: {{ line: {{ borderWidth: 2 }}, point: {{ radius: 0, hoverRadius: 3 }} }} }});
  makeChart('ch_seq_gap', 'line', {{ elements: {{ line: {{ borderWidth: 2 }}, point: {{ radius: 0, hoverRadius: 3 }} }} }});
  makeChart('ch_lead_version', 'bar', {{ indexAxis:'y', scales: {{ x: {{ beginAtZero:true }}, y: {{ ticks: {{ font: {{ size: 10 }} }} }} }} }});
  makeChart('ch_yard_occ', 'line');
  makeChart('ch_yard_zone', 'bar', {{ indexAxis:'y' }});
  makeChart('ch_charge_util', 'bar', {{ indexAxis:'y' }});
  makeChart('ch_charge_wait', 'line');
  makeChart('ch_soc', 'line');
  makeChart('ch_interrupt', 'bar', {{ indexAxis:'y' }});
  makeChart('ch_delay_cause', 'bar', {{ indexAxis:'y' }});
  makeChart('ch_shift_readiness', 'bar');
  makeChart('ch_bneck_area', 'bar', {{ indexAxis:'y' }});
  makeChart('ch_risk_matrix', 'bubble', {{ plugins: {{ legend: {{ display: false }} }} }});
  makeChart('ch_actions', 'bar', {{ indexAxis:'y' }});
  makeChart('ch_ev_vs_ice', 'bar');
  makeChart('ch_scenarios', 'bar', {{ indexAxis:'y' }});
  applyChartTheme();
}}

function init() {{
  initTheme();
  setMeta();
  setupFilters();
  renderOfficialKpis();
  initCharts();
  bind();
  updateCharts();
}}

init();
</script>
</body>
</html>
"""


def _write_dashboard_docs(official_path: Path, version: str) -> None:
    (PROJECT_ROOT / "docs" / "dashboard_architecture.md").write_text(
        """# Arquitectura del Dashboard EV (Official)

## Build Path oficial
- `python -m src.ev_build_dashboard`
- `python -m src.run_pipeline`
- Output oficial único: `outputs/dashboard/industrial-ev-operating-command-center.html`

## Principios técnicos
- KPI críticos consumidos desde dataset gobernado (`kpi_operativos.csv`).
- Sin lógica de scoring crítica en frontend.
- Payload agregado para rendimiento y legibilidad.
- Filtros aplicados por contrato de dataset.
- QA de build con manifest y reporte dedicado.
""",
        encoding="utf-8",
    )

    (PROJECT_ROOT / "docs" / "dashboard_usage.md").write_text(
        """# Uso del Dashboard Ejecutivo

1. Ejecutar pipeline oficial EV: `python -m src.run_pipeline`
2. Abrir `outputs/dashboard/industrial-ev-operating-command-center.html`
3. Aplicar filtros por fecha, turno, propulsión, versión y áreas
4. Revisar tabla de priorización y bloque de decisión ejecutiva

## Trazabilidad
- Manifest técnico: `outputs/reports/dashboard_build_manifest.json`
- Estado de release: `outputs/reports/release_readiness.json`
""",
        encoding="utf-8",
    )


def _write_manifest_and_qc(
    payload: Dict[str, object],
    output_path: Path,
    version: str,
    archived: list[str],
) -> None:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    datasets = payload["data"]
    row_counts = {k: len(v) for k, v in datasets.items()}
    html = output_path.read_text(encoding="utf-8", errors="ignore")

    priorities = payload["data"].get("priorities", [])
    top_priority_area = "N/A"
    if priorities:
        top_priority_area = sorted(
            priorities,
            key=lambda r: float(r.get("operational_priority_index") or 0.0),
            reverse=True,
        )[0].get("area", "N/A")

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
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "official_dashboard": str(output_path.relative_to(PROJECT_ROOT)),
        "html_size_bytes": html_size,
        "datasets_rows": row_counts,
        "archived_dashboards": archived,
        "checks": {
            "placeholder_free": all(tok not in html for tok in ["__PAYLOAD__", "__FILTERS__", "__CHARTJS__"]),
            "single_official_dashboard": len(list(output_path.parent.glob("*.html"))) == 1,
            "chart_js_external": "cdn.jsdelivr.net/npm/chart.js" in html,
            "kpi_payload_bound": "kpi_official" in html and "const META = PAYLOAD.meta;" in html,
            "html_size_under_6mb": html_size < 6_000_000,
            "canvas_count_expected": canvas_count == 17,
            "severity_filter_wired": "f_severity" in html and "severity:'severidad'" in html,
            "executive_snapshot_consistent": payload["meta"]["executive_snapshot"]["top_area"] == top_priority_area,
            "density_guard": density_guard,
        },
    }

    (OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    qa_lines = [
        "# Dashboard QA Report",
        "",
        f"- Versión: **{version}**",
        f"- Dashboard oficial: `{manifest['official_dashboard']}`",
        f"- Tamaño HTML: **{manifest['html_size_bytes']} bytes**",
        "",
        "## Checks",
    ]
    for k, v in manifest["checks"].items():
        qa_lines.append(f"- {k}: {'PASS' if v else 'WARN'}")

    qa_lines.extend(
        [
            "",
            "## Dataset row counts",
        ]
    )
    for k, v in row_counts.items():
        qa_lines.append(f"- {k}: {v}")

    (OUTPUT_REPORTS_DIR / "dashboard_qa_report.md").write_text("\n".join(qa_lines), encoding="utf-8")


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

    meta = _build_meta(flow, yard, charging, priorities, scenarios, kpi)
    datasets = _prepare_datasets(flow, yard, charging, dispatch, bneck, priorities, scenarios, kpi_readiness)
    payload = _build_payload(meta, datasets)

    payload_hash = hashlib.sha1(json.dumps(payload, ensure_ascii=False).encode("utf-8")).hexdigest()[:10]
    version = f"ev-official-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{payload_hash}"

    archived = _archive_non_official_dashboards(OUTPUT_DASHBOARD_DIR, OFFICIAL_DASHBOARD_NAME)

    html = _build_html(payload, version)
    output_path = OUTPUT_DASHBOARD_DIR / OFFICIAL_DASHBOARD_NAME
    output_path.write_text(html, encoding="utf-8")

    _write_dashboard_docs(output_path, version)
    _write_manifest_and_qc(payload, output_path, version, archived)

    return DashboardResult(
        path=str(output_path),
        version=version,
        payload_size_bytes=len(json.dumps(payload, ensure_ascii=False).encode("utf-8")),
    )


if __name__ == "__main__":
    result = run_ev_build_dashboard()
    print("Dashboard EV oficial generado")
    print(f"- path: {result.path}")
    print(f"- version: {result.version}")
    print(f"- payload bytes: {result.payload_size_bytes}")
