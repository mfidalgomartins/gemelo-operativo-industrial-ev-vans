from __future__ import annotations

from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from .config import DATA_PROCESSED_DIR, OUTPUT_CHARTS_DIR, OUTPUT_REPORTS_DIR, ensure_directories


sns.set_theme(style="whitegrid")


def _read_processed(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    return pd.read_csv(DATA_PROCESSED_DIR / f"{name}.csv", parse_dates=parse_dates)


def run_analysis() -> Dict[str, float]:
    ensure_directories()
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    flujo = _read_processed("fct_flujo_unidad", ["ts_prod_inicio", "ts_prod_fin", "ts_entry_patio", "ts_exit_patio", "ts_inicio_carga", "ts_fin_carga", "ts_ready_expedicion", "ts_salida_real"])
    carga = _read_processed("fct_carga_operativa")
    patio = _read_processed("fct_ocupacion_patio_hora", ["hora"])
    exped = _read_processed("fct_expedicion", ["ts_ready_expedicion", "ts_salida_real"])
    scores = _read_processed("scores_operativos")

    throughput_diario = exped.assign(fecha=exped["ts_salida_real"].dt.date).groupby("fecha").size()
    cumplimiento_sla = float(exped["cumple_sla_flag"].mean() * 100)

    kpis = {
        "throughput_diario_unidades": round(float(throughput_diario.mean()), 2),
        "lead_time_total_horas": round(float(flujo["lead_time_total_horas"].mean()), 2),
        "espera_patio_min_media": round(float(flujo["espera_patio_min"].mean()), 2),
        "espera_carga_min_media": round(float(flujo["espera_carga_min"].mean()), 2),
        "utilizacion_cargadores_pct": round(float(carga["utilizacion_cargadores_turno"].mean() * 100), 2),
        "ocupacion_patio_pct": round(float(patio["ocupacion_patio_pct"].mean()), 2),
        "cumplimiento_sla_expedicion_pct": round(cumplimiento_sla, 2),
        "indice_cuello_botella": round(float(scores["score_riesgo_cuello_botella"].mean()), 2),
        "score_readiness_operativa": round(float(scores["score_readiness_operativa"].mean()), 2),
    }

    pd.DataFrame([kpis]).to_csv(OUTPUT_REPORTS_DIR / "kpi_summary.csv", index=False)

    bottlenecks = (
        scores.groupby(["turno_referencia", "tipo_propulsion"], as_index=False)
        .agg(
            riesgo_medio=("score_riesgo_cuello_botella", "mean"),
            readiness_media=("score_readiness_operativa", "mean"),
            prioridad_media=("score_prioridad_despacho", "mean"),
            ordenes=("order_id", "count"),
        )
        .sort_values("riesgo_medio", ascending=False)
    )
    bottlenecks.to_csv(OUTPUT_REPORTS_DIR / "bottleneck_matrix.csv", index=False)

    # 1. Throughput diario
    plt.figure(figsize=(12, 4))
    throughput_diario.plot(color="#0B6E4F", linewidth=2)
    plt.title("Throughput diario de expedición")
    plt.xlabel("Fecha")
    plt.ylabel("Unidades")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "throughput_diario.png", dpi=150)
    plt.close()

    # 2. Ocupación media por hora
    patio_hora = patio.groupby("hora_dia", as_index=False)["ocupacion_patio_pct"].mean()
    plt.figure(figsize=(10, 4))
    sns.lineplot(data=patio_hora, x="hora_dia", y="ocupacion_patio_pct", marker="o", color="#D1495B")
    plt.title("Ocupación media de patio por hora del día")
    plt.xlabel("Hora")
    plt.ylabel("Ocupación %")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "ocupacion_patio_por_hora.png", dpi=150)
    plt.close()

    # 3. Utilización de cargadores por turno
    util_turno = carga.groupby("turno", as_index=False)["utilizacion_cargadores_turno"].mean()
    plt.figure(figsize=(7, 4))
    sns.barplot(data=util_turno, x="turno", y="utilizacion_cargadores_turno", palette="crest")
    plt.title("Utilización de cargadores por turno")
    plt.xlabel("Turno")
    plt.ylabel("Utilización")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "utilizacion_cargadores_turno.png", dpi=150)
    plt.close()

    # 4. SLA por destino
    sla_destino = exped.groupby("destino", as_index=False)["cumple_sla_flag"].mean()
    sla_destino["cumple_sla_flag"] *= 100
    plt.figure(figsize=(9, 4))
    sns.barplot(data=sla_destino.sort_values("cumple_sla_flag", ascending=False), x="destino", y="cumple_sla_flag", palette="mako")
    plt.title("Cumplimiento SLA por destino")
    plt.xlabel("Destino")
    plt.ylabel("Cumplimiento %")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "cumplimiento_sla_destino.png", dpi=150)
    plt.close()

    # 5. Riesgo vs readiness
    sample_scores = scores.sample(min(2500, len(scores)), random_state=42)
    plt.figure(figsize=(8, 6))
    sns.scatterplot(
        data=sample_scores,
        x="score_riesgo_cuello_botella",
        y="score_readiness_operativa",
        hue="tipo_propulsion",
        alpha=0.5,
        s=35,
    )
    plt.title("Matriz de riesgo vs readiness")
    plt.xlabel("Riesgo cuello de botella")
    plt.ylabel("Readiness operativa")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "matriz_riesgo_readiness.png", dpi=150)
    plt.close()

    # 6. Top prioridades de despacho
    top_prio = scores.nlargest(20, "score_prioridad_despacho")[
        ["order_id", "score_prioridad_despacho", "tipo_propulsion", "turno_referencia"]
    ]
    top_prio.to_csv(OUTPUT_REPORTS_DIR / "top_prioridades_despacho.csv", index=False)

    plt.figure(figsize=(12, 5))
    sns.barplot(
        data=top_prio,
        x="order_id",
        y="score_prioridad_despacho",
        hue="tipo_propulsion",
        dodge=False,
        palette="viridis",
    )
    plt.title("Top 20 órdenes por prioridad de despacho")
    plt.xlabel("Order ID")
    plt.ylabel("Score prioridad")
    plt.xticks(rotation=75)
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "top_prioridades_despacho.png", dpi=150)
    plt.close()

    return kpis


if __name__ == "__main__":
    run_analysis()
