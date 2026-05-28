from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
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


def _archive_non_official_dashboards(output_dir: Path, official_name: str) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    legacy_dir = output_dir / "legacy"
    legacy_dir.mkdir(parents=True, exist_ok=True)

    moved: list[str] = []
    for html_file in output_dir.glob("*.html"):
        if html_file.name == official_name:
            continue
        if not html_file.exists():
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
) -> dict[str, object]:
    coverage_min = pd.to_datetime(flow["fecha_real"], errors="coerce").min()
    coverage_max = pd.to_datetime(flow["fecha_real"], errors="coerce").max()

    top_area = priorities.sort_values("operational_priority_index", ascending=False).head(1)
    top_scenario = scenarios.sort_values("decision_score", ascending=False).head(1)

    kpi_row = kpi.iloc[0].to_dict() if not kpi.empty else {}
    throughput_plan_calc = int(flow["orden_id"].nunique())
    throughput_real_calc = int(flow["vehiculo_id"].nunique())
    share_ev_calc = float((flow["tipo_propulsion"] == "EV").mean()) if len(flow) else 0.0
    kpi_validation = {
        "throughput_planificado_matches_flow": int(kpi_row.get("throughput_planificado", -1)) == throughput_plan_calc,
        "throughput_real_matches_flow": int(kpi_row.get("throughput_real", -1)) == throughput_real_calc,
        "throughput_gap_matches_components": int(kpi_row.get("throughput_gap", 0))
        == int(kpi_row.get("throughput_real", 0)) - int(kpi_row.get("throughput_planificado", 0)),
        "share_ev_matches_flow": abs(float(kpi_row.get("share_ev", 0.0)) - share_ev_calc) <= 0.02,
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
        "coverage": f"{coverage_min.date()} a {coverage_max.date()}" if pd.notna(coverage_min) and pd.notna(coverage_max) else "N/A",
        "orders": int(flow["orden_id"].nunique()),
        "vehicles": int(flow["vehiculo_id"].nunique()),
        "yard_zones": int(yard["zona_patio"].nunique()),
        "charge_zones": int(charging["zona_carga"].nunique()),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "kpi_official": kpi_row,
        "kpi_validation": kpi_validation,
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
) -> dict[str, pd.DataFrame]:
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


def _build_html(payload: dict[str, object], version: str) -> str:
    return f"""<!doctype html>
<html lang=\"es\">
<head>
<meta charset=\"utf-8\" />
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
<meta name=\"dashboard-version\" content=\"{version}\" />
<title>Gemelo Operativo EV · Dashboard Oficial</title>
<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\" />
<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin />
<link rel=\"stylesheet\" href=\"https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap\" />
<script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js\"></script>
<style>
:root {{
  --font-sans:"Geist","Söhne","Helvetica Neue",Arial,sans-serif;
  --font-mono:"Geist Mono","JetBrains Mono",ui-monospace,SFMono-Regular,Menlo,monospace;

  --bg:#fafaf9;
  --surface:#ffffff;
  --surface-2:#fafaf9;
  --line:#e7e5e4;
  --line-strong:#d6d3d1;
  --ink:#1c1917;
  --ink-2:#44403c;
  --muted:#78716c;
  --subtle:#a8a29e;

  --accent:#1d4ed8;
  --accent-soft:#eff6ff;
  --accent-line:#bfdbfe;
  --danger:#b91c1c;
  --danger-soft:#fef2f2;
  --warn:#a16207;
  --positive:#15803d;

  --series-1:#1c1917;
  --series-2:#78716c;
  --series-3:#1d4ed8;
  --series-4:#b91c1c;
  --series-5:#a16207;
  --series-6:#15803d;
  --series-7:#7c3aed;
  --series-8:#0e7490;
  --series-9:#a8a29e;

  /* Semantic series aliases consumed by chart JS (themeColors).
     Mapped to the sober editorial palette so charts share the dashboard's
     visual language and remain legible in both themes. */
  --series-plan:#a8a29e;
  --series-real:#1c1917;
  --series-ev:#1d4ed8;
  --series-gap:#b91c1c;
  --series-aux:#78716c;
  --series-yard:#a16207;
  --series-load:#0e7490;
  --series-dispatch:#7c3aed;
  --series-priority:#15803d;

  --grid:#f1f1ef;
  --grid-x:#f1f1ef;
  --grid-y:#f1f1ef;
  --tooltip-bg:#0a0a0a;
  --tooltip-text:#fafafa;
  --focus:rgba(29,78,216,.24);
}}
html[data-theme='dark'] {{
  --bg:#0a0a0a;
  --surface:#0f0f0f;
  --surface-2:#141414;
  --line:#1f1f1f;
  --line-strong:#2a2a2a;
  --ink:#fafafa;
  --ink-2:#d4d4d8;
  --muted:#a1a1aa;
  --subtle:#52525b;

  --accent:#60a5fa;
  --accent-soft:#0c1a35;
  --accent-line:#1e3a8a;
  --danger:#f87171;
  --danger-soft:#2a1010;
  --warn:#fbbf24;
  --positive:#4ade80;

  --series-1:#fafafa;
  --series-2:#a1a1aa;
  --series-3:#60a5fa;
  --series-4:#f87171;
  --series-5:#fbbf24;
  --series-6:#4ade80;
  --series-7:#a78bfa;
  --series-8:#22d3ee;
  --series-9:#52525b;

  --series-plan:#52525b;
  --series-real:#fafafa;
  --series-ev:#60a5fa;
  --series-gap:#f87171;
  --series-aux:#a1a1aa;
  --series-yard:#fbbf24;
  --series-load:#22d3ee;
  --series-dispatch:#a78bfa;
  --series-priority:#4ade80;

  --grid:#1a1a1a;
  --grid-x:#1a1a1a;
  --grid-y:#1a1a1a;
  --tooltip-bg:#fafafa;
  --tooltip-text:#0a0a0a;
  --focus:rgba(96,165,250,.32);
}}

* {{ box-sizing:border-box; }}
html,body {{ margin:0; padding:0; }}
html {{ -webkit-font-smoothing:antialiased; -moz-osx-font-smoothing:grayscale; }}
body {{
  font-family:var(--font-sans);
  font-feature-settings:"ss01","cv11";
  font-size:14px;
  line-height:1.5;
  color:var(--ink);
  background:var(--bg);
}}
.sr-only {{
  position:absolute; width:1px; height:1px; padding:0; margin:-1px;
  overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0;
}}

/* ───── Top bar ───── */
.topbar {{
  position:sticky; top:0; z-index:50;
  display:flex; align-items:center; justify-content:space-between;
  gap:24px;
  padding:14px 32px;
  background:var(--surface);
  border-bottom:1px solid var(--line);
}}
.topbar-left {{ display:flex; align-items:center; gap:14px; min-width:0; }}
.brand-mark {{
  width:22px; height:22px; flex:0 0 22px;
  border-radius:5px;
  background:var(--ink);
  display:flex; align-items:center; justify-content:center;
  color:var(--surface);
  font-family:var(--font-mono);
  font-size:10px; font-weight:600;
  letter-spacing:-.02em;
}}
.brand-title {{
  font-size:13px; font-weight:500; color:var(--ink);
  letter-spacing:-.005em;
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}}
.brand-sep {{ width:1px; height:14px; background:var(--line-strong); margin:0 4px; }}
.brand-context {{ font-size:13px; color:var(--muted); white-space:nowrap; }}
.topbar-right {{ display:flex; align-items:center; gap:8px; }}
.icon-btn {{
  display:inline-flex; align-items:center; justify-content:center;
  gap:6px;
  height:30px; padding:0 11px;
  border:1px solid var(--line);
  background:var(--surface);
  color:var(--ink-2);
  border-radius:6px;
  font:500 12px/1 var(--font-sans);
  letter-spacing:-.005em;
  cursor:pointer;
  transition:border-color .12s ease, color .12s ease, background .12s ease;
}}
.icon-btn:hover {{ border-color:var(--line-strong); color:var(--ink); }}
.icon-btn svg {{ width:14px; height:14px; }}

/* ───── Main shell ───── */
.shell {{ max-width:1440px; margin:0 auto; padding:32px; }}
.shell > section + section {{ margin-top:48px; }}
.shell > .kpi-strip + section {{ margin-top:48px; }}

/* ───── Verdict ───── */
.verdict {{
  display:grid;
  grid-template-columns:minmax(0,1fr) auto;
  gap:32px;
  align-items:end;
  padding-bottom:28px;
  margin-bottom:32px;
  border-bottom:1px solid var(--line);
}}
.eyebrow {{
  font-family:var(--font-mono);
  font-size:11px;
  font-weight:500;
  letter-spacing:.04em;
  text-transform:uppercase;
  color:var(--muted);
  margin:0 0 12px 0;
}}
h1 {{
  margin:0;
  font-size:32px;
  line-height:1.15;
  font-weight:500;
  letter-spacing:-.025em;
  color:var(--ink);
  max-width:880px;
  text-wrap:balance;
}}
.verdict-sub {{
  margin:14px 0 0 0;
  font-size:15px;
  line-height:1.55;
  color:var(--muted);
  max-width:780px;
}}
.verdict-meta {{
  display:flex; flex-direction:column; align-items:flex-end; gap:8px;
  font-size:12px;
  color:var(--muted);
  white-space:nowrap;
}}
.verdict-meta strong {{ color:var(--ink); font-weight:500; font-variant-numeric:tabular-nums; }}
.verdict-meta .row {{ display:flex; gap:8px; align-items:center; }}
.verdict-meta .dot {{ width:5px; height:5px; border-radius:99px; background:var(--positive); display:inline-block; }}
.verdict-meta .dot.warn {{ background:var(--warn); }}
.verdict-meta .dot.bad {{ background:var(--danger); }}

/* ───── Filter strip ───── */
.filter-strip {{
  display:flex; align-items:center; gap:6px;
  flex-wrap:wrap;
  margin-bottom:24px;
}}
.filter-strip .label {{
  font-size:12px; color:var(--muted); margin-right:4px;
}}
.filter-chip {{
  display:inline-flex; align-items:center; gap:6px;
  padding:5px 10px 5px 10px;
  border:1px solid var(--line);
  background:var(--surface);
  border-radius:6px;
  font-size:12px;
  color:var(--ink-2);
  cursor:pointer;
}}
.filter-chip[data-active='true'] {{
  border-color:var(--line-strong);
  background:var(--surface-2);
  color:var(--ink);
}}
.filter-chip select,.filter-chip input {{
  appearance:none;
  -webkit-appearance:none;
  border:none;
  background:transparent;
  font:500 12px/1 var(--font-sans);
  color:inherit;
  cursor:pointer;
  padding:0;
  letter-spacing:-.005em;
}}
.filter-chip input[type='date'] {{
  font-family:var(--font-mono);
  font-size:11px;
  color:var(--ink);
  min-width:96px;
}}
.filter-chip select:focus,.filter-chip input:focus {{ outline:none; }}
.filter-chip strong {{ font-weight:500; color:var(--muted); font-size:11px; }}
.filter-reset {{
  margin-left:auto;
  font:500 12px/1 var(--font-sans);
  color:var(--muted);
  background:transparent; border:none; cursor:pointer;
  padding:6px 10px;
}}
.filter-reset:hover {{ color:var(--ink); }}

/* ───── KPI strip ───── */
.kpi-strip {{
  display:grid;
  grid-template-columns:repeat(7, minmax(0,1fr));
  border:1px solid var(--line);
  border-radius:8px;
  background:var(--surface);
  overflow:hidden;
}}
.kpi-cell {{
  position:relative;
  padding:20px 22px;
  border-right:1px solid var(--line);
  display:flex; flex-direction:column; gap:6px;
  min-width:0;
}}
.kpi-cell:last-child {{ border-right:none; }}
.kpi-label {{
  font-size:11px;
  color:var(--muted);
  font-weight:500;
  letter-spacing:.005em;
  white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
}}
.kpi-value {{
  font-family:var(--font-mono);
  font-size:28px;
  line-height:1.05;
  font-weight:500;
  letter-spacing:-.03em;
  color:var(--ink);
  font-variant-numeric:tabular-nums;
}}
.kpi-value.is-text {{ font-family:var(--font-sans); font-size:16px; letter-spacing:-.01em; }}
.kpi-foot {{
  margin-top:auto;
  display:flex; align-items:center; gap:6px;
  font-size:11px;
  color:var(--muted);
}}
.kpi-dot {{
  width:6px; height:6px; border-radius:99px; background:var(--subtle); flex:0 0 6px;
}}
.kpi-dot.bad {{ background:var(--danger); }}
.kpi-dot.warn {{ background:var(--warn); }}
.kpi-dot.good {{ background:var(--positive); }}

/* ───── Sections ───── */
.section-head {{
  display:flex; align-items:baseline; justify-content:space-between;
  gap:16px;
  margin-bottom:20px;
}}
.section-head h2 {{
  margin:0;
  font-size:18px;
  font-weight:500;
  letter-spacing:-.015em;
  color:var(--ink);
}}
.section-head .desc {{
  margin:6px 0 0 0;
  font-size:13px;
  color:var(--muted);
  max-width:680px;
}}
.section-head .section-num + h2 {{ margin-top:8px; }}
.section-num {{
  font-family:var(--font-mono);
  font-size:11px;
  font-weight:500;
  color:var(--subtle);
  letter-spacing:.04em;
}}

/* ───── Grid ───── */
.grid {{ display:grid; gap:16px; }}
.grid-2 {{ grid-template-columns:repeat(2, minmax(0,1fr)); }}
.grid-3 {{ grid-template-columns:repeat(3, minmax(0,1fr)); }}
.grid + .grid {{ margin-top:16px; }}

/* ───── Card ───── */
.card {{
  background:var(--surface);
  border:1px solid var(--line);
  border-radius:8px;
  padding:20px;
  display:flex; flex-direction:column;
  min-width:0;
}}
.card.tall {{ min-height:340px; }}
.card-head {{
  margin-bottom:16px;
  display:flex; align-items:flex-start; justify-content:space-between;
  gap:12px;
}}
.card-head h3 {{
  margin:0;
  font-size:14px;
  font-weight:500;
  letter-spacing:-.005em;
  color:var(--ink);
}}
.card-head .hint {{
  margin:4px 0 0 0;
  font-size:12px;
  color:var(--muted);
  max-width:48ch;
}}
.card-head .tag {{
  font-family:var(--font-mono);
  font-size:10px;
  letter-spacing:.04em;
  color:var(--subtle);
  text-transform:uppercase;
  white-space:nowrap;
}}
.card-body {{ flex:1; min-height:0; }}
.canvas-wrap {{ position:relative; height:260px; }}
canvas {{ width:100% !important; height:100% !important; }}

/* ───── Priority table ───── */
.table-card {{ padding:0; overflow:hidden; }}
.table-head-row {{
  display:flex; align-items:center; justify-content:space-between;
  gap:16px;
  padding:18px 20px;
  border-bottom:1px solid var(--line);
}}
.table-head-row h3 {{ margin:0; font-size:14px; font-weight:500; }}
.table-head-row .desc {{ margin:4px 0 0 0; font-size:12px; color:var(--muted); }}
.table-tools {{ display:flex; align-items:center; gap:8px; }}
.input,.btn {{
  height:30px;
  padding:0 11px;
  border:1px solid var(--line);
  background:var(--surface);
  color:var(--ink);
  border-radius:6px;
  font:500 12px/1 var(--font-sans);
  letter-spacing:-.005em;
}}
.input {{ min-width:240px; }}
.input::placeholder {{ color:var(--subtle); }}
.btn {{ cursor:pointer; }}
.btn:hover {{ border-color:var(--line-strong); }}
.btn-ghost {{
  background:transparent; border-color:transparent; color:var(--muted);
}}
.btn-ghost:hover {{ color:var(--ink); background:var(--surface-2); }}

.table-wrap {{ overflow:auto; max-height:520px; }}
table {{
  width:100%;
  border-collapse:collapse;
  font-size:13px;
  font-variant-numeric:tabular-nums;
}}
thead th {{
  position:sticky; top:0;
  background:var(--surface);
  border-bottom:1px solid var(--line);
  text-align:left;
  padding:10px 16px;
  font-size:11px;
  font-weight:500;
  color:var(--muted);
  letter-spacing:.02em;
  text-transform:uppercase;
  z-index:1;
}}
thead th.num {{ text-align:right; }}
tbody td {{
  padding:12px 16px;
  border-bottom:1px solid var(--line);
  color:var(--ink-2);
  vertical-align:top;
}}
tbody tr:last-child td {{ border-bottom:none; }}
tbody tr:hover td {{ background:var(--surface-2); color:var(--ink); }}
td.num {{ text-align:right; font-family:var(--font-mono); color:var(--ink); }}
td.rank {{ font-family:var(--font-mono); color:var(--subtle); font-size:12px; width:48px; }}
td .area {{ font-weight:500; color:var(--ink); display:block; }}
td .action-main {{ font-weight:500; color:var(--ink); display:block; }}
td .action-sub {{ display:block; color:var(--muted); font-size:12px; margin-top:2px; }}
.tier {{
  display:inline-flex; align-items:center;
  padding:2px 8px;
  border-radius:99px;
  font-family:var(--font-mono);
  font-size:10px;
  font-weight:500;
  letter-spacing:.04em;
  text-transform:uppercase;
}}
.tier.t0 {{ background:var(--danger-soft); color:var(--danger); }}
.tier.t1 {{ background:#fef3c7; color:var(--warn); }}
.tier.t2 {{ background:#dcfce7; color:var(--positive); }}
html[data-theme='dark'] .tier.t1 {{ background:#2a200a; color:var(--warn); }}
html[data-theme='dark'] .tier.t2 {{ background:#0f2a14; color:var(--positive); }}
.opi {{
  display:inline-block;
  font-family:var(--font-mono);
  font-size:13px;
  color:var(--ink);
  font-weight:500;
}}
.bar-cell {{ display:flex; flex-direction:column; gap:5px; min-width:120px; }}
.bar-cell .bar-track {{
  height:4px;
  background:var(--surface-2);
  border-radius:99px;
  overflow:hidden;
}}
.bar-cell .bar-fill {{
  height:100%;
  background:var(--ink);
  border-radius:99px;
}}
.bar-cell .bar-fill.t0 {{ background:var(--danger); }}
.bar-cell .bar-fill.t1 {{ background:var(--warn); }}
.bar-cell .bar-fill.t2 {{ background:var(--positive); }}
.table-status {{
  padding:12px 20px;
  border-top:1px solid var(--line);
  font-size:12px;
  color:var(--muted);
}}

/* ───── Scenario block ───── */
.scenario-head {{
  display:flex; align-items:center; gap:12px;
}}
.scenario-head select {{
  height:30px; padding:0 28px 0 11px;
  border:1px solid var(--line);
  background:var(--surface);
  color:var(--ink);
  border-radius:6px;
  font:500 12px/1 var(--font-sans);
  cursor:pointer;
}}

/* ───── Footer / decision ───── */
.decision-strip {{
  margin-top:32px;
  padding:24px 0 0 0;
  border-top:1px solid var(--line);
  display:grid;
  grid-template-columns:1fr auto auto auto auto;
  gap:32px;
  align-items:end;
}}
.decision-strip .lead {{
  font-size:14px;
  line-height:1.55;
  color:var(--ink-2);
  max-width:520px;
}}
.decision-strip .lead strong {{ color:var(--ink); font-weight:500; }}
.decision-strip .lead .eyebrow {{ margin:0 0 8px 0; }}
.decision-strip .lead #decision_text {{ display:block; }}
.decision-stat span {{
  display:block;
  font-size:11px;
  color:var(--muted);
  margin-bottom:4px;
  letter-spacing:.005em;
}}
.decision-stat strong {{
  display:block;
  font-family:var(--font-mono);
  font-size:18px;
  font-weight:500;
  color:var(--ink);
  letter-spacing:-.02em;
  white-space:nowrap;
}}
.decision-stat.text strong {{
  font-family:var(--font-sans);
  font-size:14px;
}}

footer.foot {{
  margin-top:48px;
  padding:20px 0 8px;
  border-top:1px solid var(--line);
  font-size:11px;
  color:var(--subtle);
  display:flex;
  justify-content:space-between;
  font-family:var(--font-mono);
  letter-spacing:.02em;
}}

/* ───── Focus states ───── */
button:focus-visible, select:focus-visible, input:focus-visible {{
  outline:none;
  border-color:var(--accent);
  box-shadow:0 0 0 3px var(--focus);
}}

/* ───── Responsive ───── */
@media (max-width:1280px) {{
  .kpi-strip {{ grid-template-columns:repeat(4, minmax(0,1fr)); }}
  .kpi-cell:nth-child(4) {{ border-right:none; }}
  .kpi-cell:nth-child(n+5) {{ border-top:1px solid var(--line); }}
  .grid-3 {{ grid-template-columns:repeat(2, minmax(0,1fr)); }}
}}
@media (max-width:900px) {{
  .shell {{ padding:24px 20px; }}
  .topbar {{ padding:12px 20px; }}
  .verdict {{ grid-template-columns:1fr; gap:20px; }}
  .verdict-meta {{ align-items:flex-start; }}
  h1 {{ font-size:26px; }}
  .kpi-strip {{ grid-template-columns:repeat(2, minmax(0,1fr)); }}
  .kpi-cell {{ border-right:1px solid var(--line); border-bottom:1px solid var(--line); }}
  .kpi-cell:nth-child(2n) {{ border-right:none; }}
  .grid-2,.grid-3 {{ grid-template-columns:1fr; }}
  .decision-strip {{ grid-template-columns:1fr 1fr; }}
}}
@media (max-width:600px) {{
  .brand-context, .brand-sep {{ display:none; }}
  .kpi-strip {{ grid-template-columns:1fr; }}
  .kpi-cell {{ border-right:none; }}
  .kpi-value {{ font-size:24px; }}
  .decision-strip {{ grid-template-columns:1fr; }}
}}

/* ───── Print ───── */
@media print {{
  @page {{ size:A4 landscape; margin:10mm; }}
  .topbar, .filter-strip, .table-tools, .scenario-head select, .icon-btn {{ display:none !important; }}
  body {{ background:white; }}
  .shell {{ max-width:none; padding:0; }}
  .card, .kpi-strip {{ break-inside:avoid; }}
  .canvas-wrap {{ height:220px; }}
}}
</style>
</head>
<body>
<header class=\"topbar\">
  <div class=\"topbar-left\">
    <div class=\"brand-mark\" aria-hidden=\"true\">EV</div>
    <div class=\"brand-title\">Gemelo Operativo EV</div>
    <div class=\"brand-sep\"></div>
    <div class=\"brand-context\">Dashboard de transición industrial</div>
  </div>
  <div class=\"topbar-right\">
    <button id=\"btn_toggle_filters\" class=\"icon-btn\" type=\"button\" aria-expanded=\"true\" aria-controls=\"filters_shell\">Ocultar filtros</button>
    <button id=\"btn_print\" class=\"icon-btn\" type=\"button\" aria-label=\"Exportar PDF\">Exportar</button>
    <button id=\"theme_toggle\" class=\"icon-btn\" type=\"button\" aria-label=\"Cambiar tema\">Tema</button>
  </div>
</header>

<main class=\"shell\">

<section class=\"verdict\">
  <div>
    <p class=\"eyebrow\">Lectura ejecutiva</p>
    <h1 id=\"hero_message\">El cuello dominante se ha desplazado de línea a patio, carga y expedición.</h1>
    <p class=\"verdict-sub\" id=\"verdict_sub\"></p>
  </div>
  <div class=\"verdict-meta\">
    <div class=\"row\"><span class=\"dot\" id=\"verdict_dot\"></span><span>Cobertura</span><strong id=\"meta_coverage\">—</strong></div>
    <div class=\"row\"><span>Órdenes</span><strong id=\"meta_orders\">—</strong></div>
    <div class=\"row\"><span>Vehículos</span><strong id=\"meta_vehicles\">—</strong></div>
    <div class=\"row\"><span>Zonas</span><strong><span id=\"meta_yard_zones\">—</span> patio · <span id=\"meta_charge_zones\">—</span> carga</strong></div>
  </div>
</section>

<div id=\"filters_shell\" class=\"filter-strip\" role=\"group\" aria-label=\"Filtros del dashboard\">
  <span class=\"label\">Vista:</span>
  <span class=\"filter-chip\"><strong>De</strong><input id=\"f_date_from\" type=\"date\" /></span>
  <span class=\"filter-chip\"><strong>A</strong><input id=\"f_date_to\" type=\"date\" /></span>
  <span class=\"filter-chip\"><strong>Turno</strong><select id=\"f_turno\" aria-label=\"Turno\"></select></span>
  <span class=\"filter-chip\"><strong>Propulsión</strong><select id=\"f_prop\" aria-label=\"Propulsión\"></select></span>
  <span class=\"filter-chip\"><strong>Área</strong><select id=\"f_area\" aria-label=\"Área\"></select></span>
  <span class=\"sr-only\"><select id=\"f_version\" aria-hidden=\"true\"></select></span>
  <span class=\"sr-only\"><select id=\"f_yard\" aria-hidden=\"true\"></select></span>
  <span class=\"sr-only\"><select id=\"f_charge\" aria-hidden=\"true\"></select></span>
  <span class=\"sr-only\"><select id=\"f_severity\" aria-hidden=\"true\"></select></span>
  <button id=\"btn_apply\" class=\"icon-btn\" type=\"button\">Aplicar</button>
  <button id=\"btn_reset\" class=\"filter-reset\" type=\"button\">Restablecer</button>
  <span class=\"sr-only\" id=\"filter_summary_text\"></span>
  <span class=\"sr-only\" id=\"scenario_summary_text\"></span>
</div>

<div class=\"kpi-strip\" id=\"kpi_cards\" aria-label=\"Indicadores clave\"></div>

<section class=\"flow-section\">
  <div class=\"section-head\">
    <div>
      <span class=\"section-num\">01 — Flujo &amp; Mix</span>
      <h2>Plan vs ejecución, y la presión que introduce el mix EV</h2>
      <p class=\"desc\">Si plan y real divergen y la curva EV sube, la pérdida de capacidad es estructural, no incidental.</p>
    </div>
  </div>
  <div class=\"grid grid-2\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Throughput planificado vs real</h3><p class=\"hint\">Ritmo diario; cualquier divergencia sostenida señala pérdida de capacidad.</p></div><span class=\"tag\">Diario</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_throughput\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Share EV semanal</h3><p class=\"hint\">Mix que explica la presión incremental sobre carga, patio y expedición.</p></div><span class=\"tag\">Semanal</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_ev_share\"></canvas></div></div>
    </div>
  </div>
  <div class=\"grid grid-2\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Gap de secuencia plan–real</h3><p class=\"hint\">Desviación del orden objetivo; señal temprana antes de que la cola se rompa.</p></div><span class=\"tag\">Secuencia</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_seq_gap\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Lead time por versión</h3><p class=\"hint\">Versiones con mayor tiempo interno: candidatas a arrastrar congestión.</p></div><span class=\"tag\">Complejidad</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_lead_version\"></canvas></div></div>
    </div>
  </div>
</section>

<section>
  <div class=\"section-head\">
    <div>
      <span class=\"section-num\">02 — Patio &amp; Carga</span>
      <h2>Dónde se está bloqueando físicamente el flujo</h2>
      <p class=\"desc\">Ocupación, dwell extremo, utilización de carga y SOC: las cuatro señales que precipitan el cuello de salida.</p>
    </div>
  </div>
  <div class=\"grid grid-2\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Ocupación de patio y dwell p95</h3><p class=\"hint\">Saturación + cola extrema = bloqueo físico inminente.</p></div><span class=\"tag\">Patio</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_yard_occ\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Dwell por zona</h3><p class=\"hint\">Buffers improductivos y movimientos sin valor por zona física.</p></div><span class=\"tag\">Zona</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_yard_zone\"></canvas></div></div>
    </div>
  </div>
  <div class=\"grid grid-2\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Utilización de carga</h3><p class=\"hint\">Uso medio por zona y miss-rate de SOC objetivo.</p></div><span class=\"tag\">Carga</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_charge_util\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Cola media de carga</h3><p class=\"hint\">Espera previa: señal directa del cuello energético EV.</p></div><span class=\"tag\">Cola</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_charge_wait\"></canvas></div></div>
    </div>
  </div>
  <div class=\"grid grid-2\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>SOC objetivo vs real</h3><p class=\"hint\">Gap de energía antes de expedir: clave para asignación de slots.</p></div><span class=\"tag\">Readiness</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_soc\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Interrupciones de carga</h3><p class=\"hint\">Sesiones frágiles que degradan estabilidad sin subir utilización.</p></div><span class=\"tag\">Robustez</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_interrupt\"></canvas></div></div>
    </div>
  </div>
</section>

<section>
  <div class=\"section-head\">
    <div>
      <span class=\"section-num\">03 — Riesgo &amp; Expedición</span>
      <h2>Quién pierde flujo, por qué, y cuál es el siguiente movimiento</h2>
      <p class=\"desc\">Causas de retraso, comparativa EV vs ICE, matriz de riesgo y ranking de palancas para intervenir.</p>
    </div>
  </div>
  <div class=\"grid grid-2\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Retraso por causa</h3><p class=\"hint\">Separa incidencias aisladas de fricción sistémica de salida.</p></div><span class=\"tag\">Causas</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_delay_cause\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Delay y readiness por turno</h3><p class=\"hint\">Distingue tensión localizada de problema estructural por turno.</p></div><span class=\"tag\">Turno</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_shift_readiness\"></canvas></div></div>
    </div>
  </div>
  <div class=\"grid grid-3\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Impacto de cuellos por área</h3><p class=\"hint\">Throughput perdido por área en tensión.</p></div><span class=\"tag\">Área</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_bneck_area\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Matriz de riesgo</h3><p class=\"hint\">Pérdida × riesgo de expedición: dónde actuar primero.</p></div><span class=\"tag\">Matriz</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_risk_matrix\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Ranking de acciones</h3><p class=\"hint\">Prioridad media por palanca operativa.</p></div><span class=\"tag\">Acción</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_actions\"></canvas></div></div>
    </div>
  </div>
  <div class=\"grid grid-2\">
    <div class=\"card tall\">
      <div class=\"card-head\"><div><h3>Comparativa EV vs ICE</h3><p class=\"hint\">Dónde se concentra la carga incremental del mix EV.</p></div><span class=\"tag\">Comparativa</span></div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_ev_vs_ice\"></canvas></div></div>
    </div>
    <div class=\"card tall\">
      <div class=\"card-head\">
        <div><h3>Comparador de escenarios</h3><p class=\"hint\">Qué palanca da mejor equilibrio entre throughput y riesgo.</p></div>
        <div class=\"scenario-head\"><select id=\"scenario_select\" aria-label=\"Seleccionar escenario\"></select></div>
      </div>
      <div class=\"card-body\"><div class=\"canvas-wrap\"><canvas id=\"ch_scenarios\"></canvas></div></div>
    </div>
  </div>
</section>

<section>
  <div class=\"section-head\">
    <div>
      <span class=\"section-num\">04 — Priorización operativa</span>
      <h2>Ranking de áreas y acciones para esta semana</h2>
      <p class=\"desc\">Ordenado por Operational Priority Index (OPI). Buscar por área o driver; exportar como CSV para iterar.</p>
    </div>
  </div>
  <div class=\"card table-card\">
    <div class=\"table-head-row\">
      <div>
        <h3>Áreas críticas por OPI</h3>
        <p class=\"desc\">Cada fila empareja el driver dominante con la acción recomendada.</p>
      </div>
      <div class=\"table-tools\">
        <label class=\"sr-only\" for=\"table_search\">Buscar</label>
        <input id=\"table_search\" class=\"input\" type=\"text\" placeholder=\"Buscar área, driver, acción…\" />
        <button id=\"btn_export\" class=\"btn\" type=\"button\">Exportar CSV</button>
      </div>
    </div>
    <div class=\"table-wrap\">
      <table id=\"priority_table\">
        <thead>
          <tr>
            <th class=\"rank\">#</th>
            <th>Área</th>
            <th class=\"num\">OPI</th>
            <th>Tier</th>
            <th>Driver dominante</th>
            <th>Acción recomendada</th>
            <th>Cuello asociado</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
    <div id=\"table_status\" class=\"table-status\"></div>
  </div>
</section>

<section class=\"decision-strip\">
  <div class=\"lead\">
    <span class=\"eyebrow\">Intervención recomendada</span>
    <span id=\"decision_text\"></span>
  </div>
  <div class=\"decision-stat text\"><span>Área</span><strong id=\"decision_area\">—</strong></div>
  <div class=\"decision-stat text\"><span>Acción</span><strong id=\"decision_action\">—</strong></div>
  <div class=\"decision-stat text\"><span>Driver</span><strong id=\"decision_driver\">—</strong></div>
  <div class=\"decision-stat\"><span>OPI</span><strong id=\"decision_opi\">—</strong></div>
</section>

<div class=\"sr-only\">
  <span id=\"command_matter\"></span>
  <span id=\"command_critical\"></span>
  <span id=\"command_critical_note\"></span>
  <span id=\"command_action\"></span>
  <span id=\"command_action_note\"></span>
  <span id=\"command_impact\"></span>
  <span id=\"command_impact_note\"></span>
  <span id=\"meta_top_area\"></span>
  <span id=\"meta_top_action\"></span>
  <span id=\"meta_top_scenario\"></span>
  <span id=\"meta_yard_zones_card\"></span>
  <span id=\"meta_charge_zones_card\"></span>
  <span id=\"decision_scenario\"></span>
  <ul id=\"executive_list\"></ul>
  <ul id=\"operational_list\"></ul>
</div>

<footer class=\"foot\">
  <span>Gemelo Operativo EV — v{version}</span>
  <span>Dataset gobernado · Tabular nums · pt/es</span>
</footer>

</main>

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
function mean(rows, key) {{
  if (!rows.length) return 0;
  return rows.reduce((a, r) => a + n(r[key]), 0) / rows.length;
}}
function weightedMean(rows, valueKey, weightKey) {{
  const w = rows.reduce((a, r) => a + Math.max(1, n(r[weightKey])), 0);
  if (!w) return 0;
  return rows.reduce((a, r) => a + n(r[valueKey]) * Math.max(1, n(r[weightKey])), 0) / w;
}}
function escapeHtml(v) {{
  return String(v ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}}
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
  const k = META.kpi_official || {{}};
  document.getElementById('meta_coverage').textContent = META.coverage || 'N/A';
  document.getElementById('meta_orders').textContent = (META.orders || 0).toLocaleString('es-ES');
  document.getElementById('meta_vehicles').textContent = (META.vehicles || 0).toLocaleString('es-ES');
  document.getElementById('meta_yard_zones').textContent = (META.yard_zones || 0).toLocaleString('es-ES');
  document.getElementById('meta_charge_zones').textContent = (META.charge_zones || 0).toLocaleString('es-ES');
  document.getElementById('meta_top_area').textContent = META.executive_snapshot.top_area || 'N/A';
  document.getElementById('meta_top_action').textContent = META.executive_snapshot.top_action || 'N/A';
  document.getElementById('meta_top_scenario').textContent = META.executive_snapshot.top_scenario || 'N/A';
  document.getElementById('meta_yard_zones_card').textContent = (META.yard_zones || 0).toLocaleString('es-ES');
  document.getElementById('meta_charge_zones_card').textContent = (META.charge_zones || 0).toLocaleString('es-ES');

  const list = [
    'Área con mayor presión actual: ' + (META.executive_snapshot.top_area || 'N/A') + '.',
    'Utilización media de carga: ' + pct(k.utilizacion_media_cargadores || 0) + ' con cola media de ' + n(k.tiempo_medio_espera_carga_min || 0).toFixed(1) + ' min.',
    'Patio en pico: ' + pct(k.ocupacion_pico_patio || 0) + ' y dwell p95 de ' + n(k.dwell_p95_min || 0).toFixed(0) + ' min.',
    'Escenario con mejor balance actual: ' + (META.executive_snapshot.top_scenario || 'N/A') + '.'
  ];
  document.getElementById('executive_list').innerHTML = list.map(x => '<li>' + escapeHtml(x) + '</li>').join('');

  const opList = [
    'Reservar carga para unidades con salida inmediata cuando el readiness real quede por debajo del objetivo.',
    'Reducir dwell en las zonas con mayor p95 antes de ampliar físicamente patio.',
    'Tratar la desviación de secuencia como señal temprana de rotura, no solo como efecto de la congestión.'
  ];
  document.getElementById('operational_list').innerHTML = opList.map(x => '<li>' + escapeHtml(x) + '</li>').join('');

  const topArea = META.executive_snapshot.top_area || 'N/A';
  const topAction = META.executive_snapshot.top_action || 'N/A';
  const topScenario = META.executive_snapshot.top_scenario || 'N/A';
  document.getElementById('hero_message').textContent =
    'La presión operativa se concentra en ' + topArea + '. El cuello ya no es la línea: es patio, carga y expedición.';
  const subEl = document.getElementById('verdict_sub');
  if (subEl) {{
    subEl.textContent =
      'Acción prioritaria: ' + topAction + '. Validada contra el escenario ' + topScenario + '. '
      + Math.round(n(k.vehiculos_no_ready || 0)).toLocaleString('es-ES')
      + ' vehículos llegan a expedición sin preparación suficiente, con ' + pct(k.ratio_salida_retrasada || 0)
      + ' de salidas retrasadas.';
  }}
  const dotEl = document.getElementById('verdict_dot');
  if (dotEl) {{
    const ratio = n(k.ratio_salida_retrasada || 0);
    dotEl.className = 'dot ' + (ratio > 0.12 ? 'bad' : (ratio > 0.05 ? 'warn' : ''));
  }}

  document.getElementById('command_matter').textContent = 'Proteger readiness de salida';
  document.getElementById('command_matter_note').textContent = 'El riesgo visible no es volumen total, sino vehículos que llegan a expedición sin preparación suficiente.';
  document.getElementById('command_critical').textContent = pct(k.ratio_salida_retrasada || 0) + ' de salidas retrasadas';
  document.getElementById('command_critical_note').textContent = 'Con este nivel, la prioridad es reducir cola y secuencia rota antes de perseguir más throughput.';
  document.getElementById('command_action').textContent = META.executive_snapshot.top_action || 'N/A';
  document.getElementById('command_action_note').textContent = 'Acción táctica sugerida por el ranking OPI, aplicable primero en ' + (META.executive_snapshot.top_area || 'N/A') + '.';
  document.getElementById('command_impact').textContent = Math.round(n(k.vehiculos_no_ready || 0)).toLocaleString('es-ES') + ' no ready';
  document.getElementById('command_impact_note').textContent = 'Backlog operativo que conecta carga, patio y expedición; si no baja, el ramp-up EV amplifica el cuello.';
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
  scen.innerHTML = '';
  DATA.scenarios.forEach((s, i) => {{
    const op = document.createElement('option');
    op.value = s.escenario;
    op.textContent = s.escenario;
    scen.appendChild(op);
    if (!state.scenario && i === 0) state.scenario = s.escenario;
  }});
  scen.value = state.scenario;
}}

function describeFilter(value, fallbackLabel) {{
  return value && value !== 'ALL' ? value : fallbackLabel;
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

function updateFilterSummary() {{
  const f = getFilterState();
  const parts = [
    f.from && f.to ? (f.from + ' → ' + f.to) : 'Periodo completo',
    describeFilter(f.turno, 'Todos los turnos'),
    describeFilter(f.prop, 'Todas las propulsiones'),
    describeFilter(f.area, 'Todas las áreas'),
  ];
  document.getElementById('filter_summary_text').textContent = parts.join(' · ');
  document.getElementById('scenario_summary_text').textContent = state.scenario || 'N/A';
}}

function setFilterPanelCollapsed(collapsed) {{
  const shell = document.getElementById('filters_shell');
  const btn = document.getElementById('btn_toggle_filters');
  if (!shell || !btn) return;
  shell.dataset.collapsed = collapsed ? 'true' : 'false';
  shell.style.display = collapsed ? 'none' : '';
  btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
  btn.textContent = collapsed ? 'Filtros' : 'Ocultar filtros';
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

function classifyKpi(key, value) {{
  if (key === 'throughput_gap') return value < 0 ? 'kpi-critical' : 'kpi-good';
  if (key === 'utilizacion_media_cargadores') return value > 0.82 ? 'kpi-critical' : (value > 0.68 ? 'kpi-warning' : 'kpi-good');
  if (key === 'ratio_salida_retrasada') return value > 0.12 ? 'kpi-critical' : (value > 0.05 ? 'kpi-warning' : 'kpi-good');
  if (key === 'ocupacion_pico_patio') return value > 0.85 ? 'kpi-critical' : (value > 0.70 ? 'kpi-warning' : 'kpi-good');
  if (key === 'vehiculos_no_ready') return value > 0 ? 'kpi-critical' : 'kpi-good';
  if (key === 'score_readiness_global') return value >= 70 ? 'kpi-good' : (value >= 50 ? 'kpi-warning' : 'kpi-critical');
  return 'kpi-warning';
}}

function calculateVisibleKpis(ctx) {{
  const official = META.kpi_official || {{}};
  const throughputPlan = ctx.fThrough.reduce((a, r) => a + n(r.throughput_plan), 0);
  const throughputReal = ctx.fThrough.reduce((a, r) => a + n(r.throughput_real), 0);
  const dispatchVehicles = ctx.fDispatchBase.reduce((a, r) => a + Math.max(1, n(r.vehicles)), 0);
  const delayRate = dispatchVehicles
    ? ctx.fDispatchBase.reduce((a, r) => a + n(r.delay_rate) * Math.max(1, n(r.vehicles)), 0) / dispatchVehicles
    : n(official.ratio_salida_retrasada);
  const readinessRate = dispatchVehicles
    ? ctx.fDispatchBase.reduce((a, r) => a + n(r.readiness_rate) * Math.max(1, n(r.vehicles)), 0) / dispatchVehicles
    : n(official.score_readiness_global) / 100;
  const chargeWait = mean(ctx.fCharge, 'wait');
  const chargeUtil = mean(ctx.fCharge, 'utilization');
  const yardOccPeak = ctx.fYard.length ? Math.max(...ctx.fYard.map(r => n(r.occupancy))) : n(official.ocupacion_pico_patio);
  const yardDwellP95 = mean(ctx.fYard, 'dwell_p95');
  const yardDwell = mean(ctx.fYard, 'dwell');
  const evRows = ctx.fFlowProp.filter(r => String(r.tipo_propulsion) === 'EV');
  const allFlow = ctx.fFlowProp;
  const shareEv = allFlow.reduce((a, r) => a + n(r.throughput), 0)
    ? evRows.reduce((a, r) => a + n(r.throughput), 0) / allFlow.reduce((a, r) => a + n(r.throughput), 0)
    : n(official.share_ev);
  const noReady = dispatchVehicles ? Math.round(dispatchVehicles * Math.max(0, 1 - readinessRate)) : n(official.vehiculos_no_ready);
  return {{
    throughput_real: throughputReal || n(official.throughput_real),
    throughput_gap: (throughputReal || 0) - (throughputPlan || 0),
    share_ev: shareEv,
    utilizacion_media_cargadores: chargeUtil || n(official.utilizacion_media_cargadores),
    ratio_salida_retrasada: delayRate,
    ocupacion_pico_patio: yardOccPeak,
    vehiculos_no_ready: noReady,
    tiempo_medio_patio_min: yardDwell || n(official.tiempo_medio_patio_min),
    dwell_p95_min: yardDwellP95 || n(official.dwell_p95_min),
    score_readiness_global: readinessRate * 100,
    causa_principal_cuello: official.causa_principal_cuello || 'N/A',
    tiempo_medio_espera_carga_min: chargeWait || n(official.tiempo_medio_espera_carga_min),
  }};
}}

function compactNumber(num) {{
  const v = n(num);
  const abs = Math.abs(v);
  if (abs >= 1e6) return (v / 1e6).toFixed(abs >= 1e7 ? 0 : 1).replace('.0','') + 'M';
  if (abs >= 1e4) return (v / 1e3).toFixed(0) + 'k';
  if (abs >= 1e3) return (v / 1e3).toFixed(1).replace('.0','') + 'k';
  return Math.round(v).toString();
}}
function signedCompact(num) {{
  const v = n(num);
  if (v === 0) return '0';
  return (v > 0 ? '+' : '−') + compactNumber(Math.abs(v));
}}
function pctCompact(num) {{
  return Math.round(n(num) * 100) + '%';
}}
function tone(key, value) {{
  if (key === 'throughput_gap') return n(value) < 0 ? 'bad' : 'good';
  if (key === 'ratio_salida_retrasada') return n(value) > 0.12 ? 'bad' : (n(value) > 0.05 ? 'warn' : 'good');
  if (key === 'utilizacion_media_cargadores') return n(value) > 0.82 ? 'bad' : (n(value) > 0.68 ? 'warn' : 'good');
  if (key === 'ocupacion_pico_patio') return n(value) > 0.85 ? 'bad' : (n(value) > 0.70 ? 'warn' : 'good');
  if (key === 'vehiculos_no_ready') return n(value) > 0 ? 'warn' : 'good';
  if (key === 'score_readiness_global') return n(value) >= 70 ? 'good' : (n(value) >= 50 ? 'warn' : 'bad');
  return '';
}}

function renderOfficialKpis(k = META.kpi_official || {{}}) {{
  // 7 KPIs above the fold — selected for executive decision-making
  const cells = [
    {{
      key:'throughput_real',
      label:'Throughput real',
      value:compactNumber(k.throughput_real || 0),
      foot:'unidades · período completo',
    }},
    {{
      key:'throughput_gap',
      label:'Gap vs plan',
      value:signedCompact(k.throughput_gap || 0),
      foot:n(k.throughput_gap || 0) < 0 ? 'déficit operativo' : 'sin déficit neto',
    }},
    {{
      key:'ratio_salida_retrasada',
      label:'Salidas retrasadas',
      value:pctCompact(k.ratio_salida_retrasada || 0),
      foot:'sobre vehículos despachados',
    }},
    {{
      key:'vehiculos_no_ready',
      label:'No-ready a expedición',
      value:compactNumber(k.vehiculos_no_ready || 0),
      foot:'backlog accionable',
    }},
    {{
      key:'tiempo_medio_espera_carga_min',
      label:'Espera media carga',
      value:n(k.tiempo_medio_espera_carga_min || 0).toFixed(1),
      unit:'min',
      foot:'cola antes de energía disponible',
    }},
    {{
      key:'ocupacion_pico_patio',
      label:'Ocupación pico patio',
      value:pctCompact(k.ocupacion_pico_patio || 0),
      foot:'p95 dwell ' + Math.round(n(k.dwell_p95_min || 0)) + ' min',
    }},
    {{
      key:'share_ev',
      label:'Share EV',
      value:pctCompact(k.share_ev || 0),
      foot:'mix actual del flujo',
    }},
  ];
  document.getElementById('kpi_cards').innerHTML = cells.map(c => {{
    const t = tone(c.key, k[c.key]);
    const valueHtml = c.unit
      ? escapeHtml(c.value) + '<span style="font-family:var(--font-sans);font-size:14px;color:var(--muted);font-weight:400;margin-left:4px;">' + escapeHtml(c.unit) + '</span>'
      : escapeHtml(c.value);
    return '<div class="kpi-cell">'
      + '<span class="kpi-label">' + escapeHtml(c.label) + '</span>'
      + '<span class="kpi-value">' + valueHtml + '</span>'
      + '<span class="kpi-foot"><span class="kpi-dot ' + t + '"></span>' + escapeHtml(c.foot) + '</span>'
      + '</div>';
  }}).join('');
}}

function applyTheme(theme) {{
  document.documentElement.setAttribute('data-theme', theme);
  try {{ localStorage.setItem(THEME_KEY, theme); }} catch (e) {{}}
  const btn = document.getElementById('theme_toggle');
  if (btn) btn.textContent = theme === 'dark' ? 'Claro' : 'Oscuro';
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
          grid: {{ color: c.gridX, drawBorder: false }}
        }},
        y: {{
          ticks: {{ font: {{ size: 10 }}, color: c.muted }},
          grid: {{ color: c.gridY, drawBorder: false }}
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
  updateFilterSummary();
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
  const filterState = getFilterState();
  const hasActiveContext = Object.entries(filterState).some(([key, value]) => {{
    if (key === 'from' || key === 'to') return false;
    return value && value !== 'ALL';
  }});
  renderOfficialKpis(
    calculateVisibleKpis({{ fThrough, fDispatchBase, fCharge, fYard, fFlowProp }}),
    hasActiveContext ? 'contexto filtrado' : 'periodo completo'
  );
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
  body.innerHTML = data.map((r, idx) =>
    '<tr>' +
    '<td><span class="score-badge">' + (idx + 1) + '</span></td>' +
    '<td>' + escapeHtml(r.area || 'N/A') + '</td>' +
    '<td><span class="score-badge">' + n(r.operational_priority_index).toFixed(1) + '</span></td>' +
    '<td><span class="tier-badge ' + tierClass(r.area_priority_tier) + '">' + escapeHtml(r.area_priority_tier || 'N/A') + '</span></td>' +
    '<td>' + escapeHtml(r.main_risk_driver || 'N/A') + '</td>' +
    '<td class="action-cell"><strong>' + escapeHtml(r.recommended_action || 'N/A') + '</strong><span>Riesgo: ' + escapeHtml(r.main_risk_driver || 'N/A') + '</span></td>' +
    '<td>' + escapeHtml(r.main_bottleneck_driver || 'N/A') + '</td>' +
    '</tr>'
  ).join('');

  document.getElementById('table_status').textContent = 'Filas visibles: ' + data.length.toLocaleString('es-ES');

  const top = data[0];
  if (top) {{
    document.getElementById('decision_area').textContent = top.area || 'N/A';
    document.getElementById('decision_action').textContent = top.recommended_action || 'N/A';
    document.getElementById('decision_driver').textContent = top.main_risk_driver || 'N/A';
    document.getElementById('decision_opi').textContent = n(top.operational_priority_index).toFixed(1);
    document.getElementById('decision_scenario').textContent = 'Escenario recomendado: ' + (state.scenario || 'N/A');
    document.getElementById('decision_text').textContent =
      'La intervención inicial debe concentrarse en ' + top.area + '. '
      + 'La palanca con mejor retorno operativo inmediato es ' + (top.recommended_action || 'N/A')
      + ', porque ataca el driver dominante (' + (top.main_risk_driver || 'N/A') + ') '
      + 'sin exigir una expansión indiscriminada de capacidad.';
  }} else {{
    document.getElementById('decision_area').textContent = 'N/A';
    document.getElementById('decision_action').textContent = 'N/A';
    document.getElementById('decision_driver').textContent = 'N/A';
    document.getElementById('decision_opi').textContent = 'N/A';
    document.getElementById('decision_scenario').textContent = 'Escenario recomendado: N/A';
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
    state.scenario = DATA.scenarios[0] ? DATA.scenarios[0].escenario : '';
    setupFilters();
    document.getElementById('table_search').value = '';
    updateCharts();
  }});
  document.getElementById('btn_toggle_filters').addEventListener('click', () => {{
    const collapsed = document.getElementById('filters_shell').dataset.collapsed === 'true';
    setFilterPanelCollapsed(!collapsed);
  }});
  document.getElementById('table_search').addEventListener('input', () => renderPriorityTable(filterRows(DATA.priorities, {{ area:'area' }})));
  document.getElementById('btn_export').addEventListener('click', exportCsv);
  document.getElementById('scenario_select').addEventListener('change', (e) => {{ state.scenario = e.target.value; updateCharts(); }});
  document.getElementById('btn_print').addEventListener('click', () => window.print());
}}

function initCharts() {{
  const c = themeColors();
  Chart.defaults.font.family = 'Geist, Söhne, Helvetica Neue, Arial, sans-serif';
  Chart.defaults.font.size = 11;
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
  setFilterPanelCollapsed(false);
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
- QA de build con manifest técnico.
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
- Manifest técnico de build: `outputs/reports/dashboard_build_manifest.json`
- Estado de release: `outputs/reports/release_readiness.json`
""",
        encoding="utf-8",
    )


def _write_manifest_and_qc(
    payload: dict[str, object],
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
        "kpi_validation": payload["meta"].get("kpi_validation", {}),
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
            "kpi_logic_valid": all(payload["meta"].get("kpi_validation", {}).values()),
        },
    }

    (OUTPUT_REPORTS_DIR / "dashboard_build_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


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
