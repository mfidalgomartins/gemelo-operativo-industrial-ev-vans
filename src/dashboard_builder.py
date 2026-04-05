from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

from .config import DATA_PROCESSED_DIR, OUTPUT_DASHBOARD_DIR, OUTPUT_REPORTS_DIR, ensure_directories


def _kpi_cards(kpi_row: pd.Series) -> str:
    mapping = {
        "Throughput diario": f"{kpi_row['throughput_diario_unidades']}",
        "Lead time total (h)": f"{kpi_row['lead_time_total_horas']}",
        "Cumplimiento SLA (%)": f"{kpi_row['cumplimiento_sla_expedicion_pct']}",
        "Readiness media": f"{kpi_row['score_readiness_operativa']}",
        "Riesgo medio": f"{kpi_row['indice_cuello_botella']}",
        "Ocupación patio (%)": f"{kpi_row['ocupacion_patio_pct']}",
    }
    cards = []
    for title, value in mapping.items():
        cards.append(
            "<div class='kpi'>"
            f"<div class='kpi-label'>{title}</div>"
            f"<div class='kpi-value'>{value}</div>"
            "</div>"
        )
    return "".join(cards)


def build_dashboard() -> str:
    ensure_directories()
    OUTPUT_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    kpis = pd.read_csv(OUTPUT_REPORTS_DIR / "kpi_summary.csv").iloc[0]
    scores = pd.read_csv(DATA_PROCESSED_DIR / "scores_operativos.csv")
    exped = pd.read_csv(DATA_PROCESSED_DIR / "fct_expedicion.csv", parse_dates=["ts_salida_real"])
    patio = pd.read_csv(DATA_PROCESSED_DIR / "fct_ocupacion_patio_hora.csv")
    escenarios = pd.read_csv(DATA_PROCESSED_DIR / "scenario_resultados.csv")

    throughput = (
        exped.assign(fecha=exped["ts_salida_real"].dt.date)
        .groupby("fecha")
        .size()
        .reset_index(name="unidades")
    )

    fig_throughput = px.line(
        throughput,
        x="fecha",
        y="unidades",
        title="Throughput diario de expedición",
        color_discrete_sequence=["#0a6d8a"],
    )
    fig_throughput.update_layout(margin=dict(l=30, r=20, t=48, b=30), plot_bgcolor="#ffffff", paper_bgcolor="#ffffff")

    top_prio = scores.nlargest(15, "score_prioridad_despacho")
    fig_priority = px.bar(
        top_prio,
        x="order_id",
        y="score_prioridad_despacho",
        color="tipo_propulsion",
        title="Top órdenes para priorización operativa",
        color_discrete_sequence=["#1f6f8b", "#b05247"],
    )
    fig_priority.update_layout(margin=dict(l=30, r=20, t=48, b=30), plot_bgcolor="#ffffff", paper_bgcolor="#ffffff")

    fig_scatter = px.scatter(
        scores.sample(min(2000, len(scores)), random_state=42),
        x="score_riesgo_cuello_botella",
        y="score_readiness_operativa",
        color="tipo_propulsion",
        title="Matriz riesgo vs readiness",
        opacity=0.55,
        color_discrete_sequence=["#264653", "#e76f51"],
    )
    fig_scatter.update_layout(margin=dict(l=30, r=20, t=48, b=30), plot_bgcolor="#ffffff", paper_bgcolor="#ffffff")

    patio_hour = patio.groupby("hora_dia", as_index=False)["ocupacion_patio_pct"].mean()
    fig_patio = px.area(
        patio_hour,
        x="hora_dia",
        y="ocupacion_patio_pct",
        title="Ocupación media de patio por hora",
        color_discrete_sequence=["#d67b2a"],
    )
    fig_patio.update_layout(margin=dict(l=30, r=20, t=48, b=30), plot_bgcolor="#ffffff", paper_bgcolor="#ffffff")

    fig_scenarios = go.Figure()
    fig_scenarios.add_trace(
        go.Scatter(
            x=escenarios["mix_ev_pct"],
            y=escenarios["throughput_diario_unidades"],
            mode="lines+markers",
            name="Throughput",
            line=dict(color="#2f7d4d", width=3),
        )
    )
    fig_scenarios.add_trace(
        go.Scatter(
            x=escenarios["mix_ev_pct"],
            y=escenarios["cumplimiento_sla_pct"],
            mode="lines+markers",
            name="Cumplimiento SLA",
            line=dict(color="#ba2d3f", width=3),
        )
    )
    fig_scenarios.update_layout(
        title="Escenarios de transición EV",
        xaxis_title="Mix EV (%)",
        yaxis_title="Nivel",
        margin=dict(l=30, r=20, t=48, b=30),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
    )

    html = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Gemelo Operativo EV · Dashboard Legacy Mejorado</title>
  <style>
    :root {
      --bg-a:#f4efe6;
      --bg-b:#edf3f9;
      --ink:#11243b;
      --line:#d4dce7;
      --card:#ffffff;
      --accent:#0a6d8a;
    }
    * { box-sizing:border-box; }
    body {
      margin:0;
      font-family:"IBM Plex Sans","Avenir Next","Segoe UI",sans-serif;
      color:var(--ink);
      background:linear-gradient(120deg,var(--bg-a),var(--bg-b));
    }
    .wrap { max-width:1440px; margin:0 auto; padding:18px; }
    .panel { background:var(--card); border:1px solid var(--line); border-radius:14px; padding:14px; box-shadow:0 7px 20px rgba(0,0,0,.08); }
    .head { display:flex; justify-content:space-between; gap:12px; align-items:flex-start; flex-wrap:wrap; }
    h1 { margin:0; font-size:30px; }
    .sub { margin:6px 0 0 0; color:#4c6079; max-width:980px; line-height:1.35; }
    .banner {
      margin-top:10px;
      background:#eef7fb;
      border:1px solid #c9dcea;
      border-radius:10px;
      padding:9px 10px;
      font-size:13px;
    }
    .kpis { margin-top:12px; display:grid; grid-template-columns:repeat(6,minmax(140px,1fr)); gap:10px; }
    .kpi { border:1px solid var(--line); border-radius:10px; padding:10px; background:#fbfdff; }
    .kpi-label { font-size:12px; color:#5b6f88; }
    .kpi-value { margin-top:5px; font-size:22px; font-weight:700; }
    .grid-2 { margin-top:12px; display:grid; grid-template-columns:1fr 1fr; gap:10px; }
    .section-title { margin:0 0 6px 0; font-size:19px; }
    .plot-box { border:1px solid #e5ebf3; border-radius:12px; background:#fff; padding:8px; }
    .footer { margin-top:10px; text-align:right; font-size:11px; color:#607288; }
    @media (max-width:1000px) {
      .kpis { grid-template-columns:repeat(2,minmax(140px,1fr)); }
      .grid-2 { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <div class="head">
        <div>
          <h1>Gemelo Operativo de Lanzamiento Industrial EV</h1>
          <p class="sub">Versión legacy mejorada con foco en continuidad histórica. Para la pieza principal de portfolio usar <code>dashboard_gemelo_operativo_ev.html</code>.</p>
        </div>
      </div>
      <div class="banner">
        Este dashboard mantiene la capa histórica previa y ahora comparte criterio visual/ejecutivo con la versión premium.
      </div>
      <div class="kpis">
        __KPIS__
      </div>
    </div>

    <div class="panel" style="margin-top:12px;">
      <h2 class="section-title">Flujo y desempeño</h2>
      <div class="grid-2">
        <div class="plot-box">__THROUGHPUT__</div>
        <div class="plot-box">__PATIO__</div>
      </div>
    </div>

    <div class="panel" style="margin-top:12px;">
      <h2 class="section-title">Riesgo y priorización</h2>
      <div class="grid-2">
        <div class="plot-box">__SCATTER__</div>
        <div class="plot-box">__PRIORITY__</div>
      </div>
    </div>

    <div class="panel" style="margin-top:12px;">
      <h2 class="section-title">Scenario Planning</h2>
      <div class="plot-box">__SCENARIOS__</div>
    </div>

    <div class="footer">Dashboard legacy mejorado para consistencia visual y lectura ejecutiva.</div>
  </div>
</body>
</html>
"""

    html = html.replace("__KPIS__", _kpi_cards(kpis))
    html = html.replace("__THROUGHPUT__", pio.to_html(fig_throughput, include_plotlyjs="cdn", full_html=False))
    html = html.replace("__PATIO__", pio.to_html(fig_patio, include_plotlyjs=False, full_html=False))
    html = html.replace("__SCATTER__", pio.to_html(fig_scatter, include_plotlyjs=False, full_html=False))
    html = html.replace("__PRIORITY__", pio.to_html(fig_priority, include_plotlyjs=False, full_html=False))
    html = html.replace("__SCENARIOS__", pio.to_html(fig_scenarios, include_plotlyjs=False, full_html=False))

    # Legacy dashboard deprecated: manter compatibilidade do pipeline sem gerar HTML legado.
    legacy_html_path = OUTPUT_DASHBOARD_DIR / "legacy" / "dashboard_gemelo_operativo.html"
    if legacy_html_path.exists():
        legacy_html_path.unlink()

    deprecation_path = OUTPUT_REPORTS_DIR / "dashboard_legacy_deprecated.md"
    deprecation_path.write_text(
        """# Legacy Dashboard Deprecated

O dashboard legacy foi descontinuado para evitar duplicidade de fonte visual e risco de inconsistência.

- Dashboard oficial único: `outputs/dashboard/dashboard_gemelo_operativo_ev.html`
- Build oficial: `python -m src.ev_build_dashboard`
""",
        encoding="utf-8",
    )
    return deprecation_path.as_posix()


if __name__ == "__main__":
    build_dashboard()
