from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from .config import DATA_PROCESSED_DIR, OUTPUT_CHARTS_DIR, OUTPUT_REPORTS_DIR


sns.set_theme(style="whitegrid")
EV_DIR = DATA_PROCESSED_DIR / "ev_factory"


@dataclass
class VizResult:
    charts_generated: int


def _read(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = EV_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe tabla para visualización: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def run_ev_create_visuals() -> VizResult:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    vehicle = _read("vehicle_readiness_features", parse_dates=["fecha_real"])
    flow = _read("vw_vehicle_flow_timeline", parse_dates=["fecha_programada", "fecha_real"])
    yard = _read("yard_features", parse_dates=["timestamp"])
    charging = _read("charging_features", parse_dates=["fecha"])
    dispatch = _read("vw_dispatch_readiness", parse_dates=["fecha"])
    bottleneck = _read("vw_shift_bottleneck_summary", parse_dates=["fecha"])
    area_rank = _read("diagnostic_area_ranking")
    shift_comp = _read("diagnostic_shift_comparison")
    ev_comp = _read("diagnostic_ev_vs_non_ev")
    scenarios = _read("scenario_table")
    actions = pd.read_csv(OUTPUT_REPORTS_DIR / "top_acciones_recomendadas.csv")

    charts: List[Dict[str, str]] = []

    # 1 throughput planificado vs real
    planned = flow.groupby(flow["fecha_programada"].dt.date).size().rename("plan")
    real = flow.groupby(flow["fecha_real"].dt.date).size().rename("real")
    tr = pd.concat([planned, real], axis=1).fillna(0).reset_index().rename(columns={"index": "fecha"})
    plt.figure(figsize=(12, 5))
    plt.plot(tr["fecha"], tr["plan"], label="Planificado", linewidth=2)
    plt.plot(tr["fecha"], tr["real"], label="Real", linewidth=2)
    plt.title("Throughput diario: planificado vs real")
    plt.xlabel("Fecha")
    plt.ylabel("Vehículos")
    plt.legend()
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_01_throughput_plan_vs_real.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Throughput plan vs real"})

    # 2 share EV por semana
    week_ev = vehicle.assign(week=vehicle["fecha_real"].dt.to_period("W").dt.start_time)
    week_ev = week_ev.groupby("week")["tipo_propulsion"].apply(lambda s: (s == "EV").mean()).reset_index(name="share_ev")
    plt.figure(figsize=(11, 4))
    sns.lineplot(data=week_ev, x="week", y="share_ev", marker="o", color="#1f78b4")
    plt.title("Transición EV: share semanal de producción")
    plt.xlabel("Semana")
    plt.ylabel("Share EV")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_02_share_ev_semanal.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Share EV por semana"})

    # 3 tiempo total interno por versión
    plt.figure(figsize=(11, 5))
    sns.boxplot(data=vehicle, x="version_id", y="total_internal_lead_time", hue="tipo_propulsion")
    plt.title("Tiempo total interno por versión")
    plt.xlabel("Versión")
    plt.ylabel("Minutos")
    plt.xticks(rotation=25)
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_03_tiempo_total_interno_por_version.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Tiempo total interno por versión"})

    # 4 secuencia planificada vs real
    seq = flow[["secuencia_planeada", "secuencia_real", "tipo_propulsion"]].sample(min(3000, len(flow)), random_state=42)
    plt.figure(figsize=(7, 7))
    sns.scatterplot(data=seq, x="secuencia_planeada", y="secuencia_real", hue="tipo_propulsion", alpha=0.4, s=15)
    plt.title("Disrupción de secuencia: planificada vs real")
    plt.xlabel("Secuencia planificada")
    plt.ylabel("Secuencia real")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_04_secuencia_plan_vs_real.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Secuencia planificada vs real"})

    # 5 ocupación del patio en el tiempo
    occ_time = yard.groupby(yard["timestamp"].dt.floor("D"))["yard_occupancy_rate"].mean().reset_index()
    plt.figure(figsize=(12, 5))
    sns.lineplot(data=occ_time, x="timestamp", y="yard_occupancy_rate", color="#e31a1c")
    plt.title("Patio: ocupación media diaria")
    plt.xlabel("Fecha")
    plt.ylabel("Ocupación")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_05_ocupacion_patio_tiempo.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Ocupación del patio en el tiempo"})

    # 6 dwell time por zona de patio
    plt.figure(figsize=(9, 5))
    sns.barplot(data=yard.groupby("zona_patio", as_index=False)["avg_dwell_time"].mean(), x="zona_patio", y="avg_dwell_time", color="#33a02c")
    plt.title("Dwell time medio por zona de patio")
    plt.xlabel("Zona patio")
    plt.ylabel("Minutos")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_06_dwell_time_por_zona.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Dwell time por zona"})

    # 7 blocking rate por zona
    plt.figure(figsize=(9, 5))
    sns.barplot(data=yard.groupby("zona_patio", as_index=False)["blocking_rate"].mean(), x="zona_patio", y="blocking_rate", color="#ff7f00")
    plt.title("Blocking rate por zona de patio")
    plt.xlabel("Zona patio")
    plt.ylabel("Blocking rate")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_07_blocking_rate_por_zona.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Blocking rate por zona"})

    # 8 movimientos no productivos
    np_moves = flow.groupby("turno", as_index=False)["non_productive_moves_count"].mean()
    plt.figure(figsize=(7, 4))
    sns.barplot(data=np_moves, x="turno", y="non_productive_moves_count", color="#6a3d9a")
    plt.title("Movimientos no productivos por turno")
    plt.xlabel("Turno")
    plt.ylabel("Media movimientos")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_08_movimientos_no_productivos.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Movimientos no productivos"})

    # 9 utilización de slots de carga
    plt.figure(figsize=(11, 5))
    sns.boxplot(data=charging, x="turno", y="charger_pressure_score", hue="zona_carga")
    plt.title("Utilización/presión de slots de carga por turno")
    plt.xlabel("Turno")
    plt.ylabel("Charger pressure score")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_09_utilizacion_slots_carga.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Utilización de slots de carga"})

    # 10 cola media antes de carga
    wait_q = charging.groupby(["fecha", "turno"], as_index=False)["avg_wait_to_charge"].mean()
    plt.figure(figsize=(12, 4))
    sns.lineplot(data=wait_q, x="fecha", y="avg_wait_to_charge", hue="turno")
    plt.title("Cola media antes de carga")
    plt.xlabel("Fecha")
    plt.ylabel("Minutos")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_10_cola_media_carga.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Cola media antes de carga"})

    # 11 SOC objetivo vs SOC real a salida
    soc = dispatch[["soc_salida_pct", "target_soc_salida_pct"]].sample(min(4000, len(dispatch)), random_state=1)
    plt.figure(figsize=(7, 6))
    sns.scatterplot(data=soc, x="target_soc_salida_pct", y="soc_salida_pct", alpha=0.4, s=15)
    plt.title("SOC objetivo vs SOC real a salida")
    plt.xlabel("SOC objetivo")
    plt.ylabel("SOC real")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_11_soc_objetivo_vs_real.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "SOC objetivo vs SOC real"})

    # 12 retrasos de expedición por causa
    delay_cause = dispatch.groupby("causa_retraso", as_index=False)["dispatch_delay_min"].mean().sort_values("dispatch_delay_min", ascending=False).head(10)
    plt.figure(figsize=(10, 5))
    sns.barplot(data=delay_cause, y="causa_retraso", x="dispatch_delay_min", color="#b15928")
    plt.title("Retrasos de expedición por causa")
    plt.xlabel("Retraso medio (min)")
    plt.ylabel("Causa")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_12_retrasos_expedicion_por_causa.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Retrasos por causa"})

    # 13 cuellos de botella por área
    b_area = bottleneck.groupby("area", as_index=False)["impacto_throughput_total"].sum().sort_values("impacto_throughput_total", ascending=False)
    plt.figure(figsize=(9, 5))
    sns.barplot(data=b_area, x="area", y="impacto_throughput_total", color="#a6cee3")
    plt.title("Impacto acumulado de cuellos por área")
    plt.xlabel("Área")
    plt.ylabel("Impacto throughput")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_13_cuellos_por_area.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Cuellos de botella por área"})

    # 14 presión operativa por turno
    shift_m = shift_comp.melt(id_vars=["turno"], var_name="score", value_name="value")
    plt.figure(figsize=(10, 5))
    sns.lineplot(data=shift_m, x="turno", y="value", hue="score", marker="o")
    plt.title("Presión operativa por turno")
    plt.xlabel("Turno")
    plt.ylabel("Score")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_14_presion_operativa_por_turno.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Presión operativa por turno"})

    # 15 matriz riesgo de área
    plt.figure(figsize=(8, 6))
    sns.scatterplot(
        data=area_rank,
        x="throughput_gap",
        y="avg_wait_time",
        size="area_criticality_score",
        hue="main_bottleneck_driver",
        sizes=(80, 600),
        alpha=0.75,
    )
    plt.title("Matriz de riesgo por área")
    plt.xlabel("Throughput gap")
    plt.ylabel("Avg wait time")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_15_matriz_riesgo_area.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Matriz riesgo de área"})

    # 16 comparación EV vs no EV
    ev_m = ev_comp.melt(id_vars=["tipo_propulsion"], var_name="score", value_name="value")
    plt.figure(figsize=(10, 5))
    sns.barplot(data=ev_m, x="score", y="value", hue="tipo_propulsion")
    plt.title("Comparación EV vs no EV")
    plt.xlabel("Score")
    plt.ylabel("Valor")
    plt.xticks(rotation=25)
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_16_comparacion_ev_vs_noev.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Comparación EV vs no EV"})

    # 17 impacto de escenarios
    s_plot = scenarios[["escenario", "throughput", "estabilidad_operativa", "espera_carga"]].copy()
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(s_plot["escenario"], s_plot["throughput"], marker="o", color="#1b9e77", label="Throughput")
    ax1.plot(s_plot["escenario"], s_plot["estabilidad_operativa"], marker="o", color="#d95f02", label="Estabilidad")
    ax1.set_ylabel("Nivel")
    ax1.tick_params(axis="x", rotation=35)
    ax2 = ax1.twinx()
    ax2.bar(s_plot["escenario"], s_plot["espera_carga"], alpha=0.25, color="#7570b3", label="Espera carga")
    ax2.set_ylabel("Espera carga (min)")
    ax1.set_title("Impacto operativo por escenario")
    fig.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_17_impacto_escenarios.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Impacto de escenarios"})

    # 18 ranking de acciones recomendadas
    plt.figure(figsize=(10, 4))
    sns.barplot(data=actions, y="recommended_action", x="prioridad_media", color="#fb9a99")
    plt.title("Ranking de acciones recomendadas")
    plt.xlabel("Prioridad media")
    plt.ylabel("Acción")
    plt.tight_layout()
    f = OUTPUT_CHARTS_DIR / "ev_18_ranking_acciones.png"
    plt.savefig(f, dpi=160)
    plt.close()
    charts.append({"archivo": f.name, "descripcion": "Ranking de acciones recomendadas"})

    # Índice de gráficos
    idx = pd.DataFrame(charts)
    idx.to_csv(OUTPUT_CHARTS_DIR / "ev_chart_index.csv", index=False)

    expl_lines = [
        "# Índice de Visualizaciones EV",
        "",
    ]
    for i, ch in enumerate(charts, start=1):
        expl_lines.append(f"{i}. {ch['archivo']}: {ch['descripcion']}")

    (OUTPUT_REPORTS_DIR / "visualizations_index.md").write_text("\n".join(expl_lines), encoding="utf-8")

    return VizResult(charts_generated=len(charts))


if __name__ == "__main__":
    result = run_ev_create_visuals()
    print("Visualizaciones EV generadas")
    print(f"- charts: {result.charts_generated}")
