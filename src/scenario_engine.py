from __future__ import annotations

from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from .config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUT_CHARTS_DIR, OUTPUT_REPORTS_DIR, ensure_directories


sns.set_theme(style="whitegrid")


def run_scenario_engine() -> pd.DataFrame:
    ensure_directories()

    ordenes = pd.read_csv(DATA_RAW_DIR / "ordenes_produccion.csv")
    kpis = pd.read_csv(OUTPUT_REPORTS_DIR / "kpi_summary.csv").iloc[0].to_dict()

    features = pd.read_csv(DATA_PROCESSED_DIR / "features_operativas.csv")
    base_ev = float((ordenes["tipo_propulsion"] == "EV").mean())
    base_wait_charge = float(features["espera_carga_min"].mean())
    base_wait_total = float(features["espera_total_min"].mean())
    base_congestion = float(features["indice_congestion_patio"].mean() * 100)

    scenarios: List[Dict[str, float]] = [
        {"escenario": "Base actual", "ev_mix": base_ev, "factor_cargadores": 1.0, "factor_energia": 1.0, "factor_patio": 1.0},
        {"escenario": "EV70_sin_refuerzo", "ev_mix": 0.70, "factor_cargadores": 1.0, "factor_energia": 1.0, "factor_patio": 1.0},
        {"escenario": "EV70_cargadores_20pct", "ev_mix": 0.70, "factor_cargadores": 1.2, "factor_energia": 1.0, "factor_patio": 1.0},
        {"escenario": "EV80_cargadores_35pct", "ev_mix": 0.80, "factor_cargadores": 1.35, "factor_energia": 1.1, "factor_patio": 1.05},
        {"escenario": "EV85_refuerzo_integral", "ev_mix": 0.85, "factor_cargadores": 1.50, "factor_energia": 1.2, "factor_patio": 1.1},
    ]

    out_rows: List[Dict[str, float]] = []
    for sc in scenarios:
        ratio_load = (sc["ev_mix"] / max(base_ev, 0.01)) / (sc["factor_cargadores"] * sc["factor_energia"])

        espera_carga = base_wait_charge * (ratio_load ** 1.45)
        espera_total = base_wait_total * (0.82 + 0.18 * ratio_load) / sc["factor_patio"]
        ocupacion_patio = min(99.0, base_congestion * (1 + (sc["ev_mix"] - base_ev) * 0.7) / sc["factor_patio"])

        cumplimiento_sla = max(
            45.0,
            float(kpis["cumplimiento_sla_expedicion_pct"])
            - 0.09 * max(0.0, espera_carga - base_wait_charge)
            - 0.35 * max(0.0, ocupacion_patio - base_congestion),
        )

        throughput = max(
            35.0,
            float(kpis["throughput_diario_unidades"])
            * (1 - 0.0012 * max(0.0, espera_total - base_wait_total))
            * (1 - 0.003 * max(0.0, ocupacion_patio - 85)),
        )

        riesgo = min(99.0, 20 + 0.5 * ocupacion_patio + 0.15 * espera_carga)
        readiness = max(5.0, 100 - 0.6 * riesgo + 0.2 * sc["factor_energia"] * 10)

        out_rows.append(
            {
                "escenario": sc["escenario"],
                "mix_ev_pct": round(sc["ev_mix"] * 100, 1),
                "espera_carga_min": round(espera_carga, 2),
                "espera_total_min": round(espera_total, 2),
                "ocupacion_patio_pct": round(ocupacion_patio, 2),
                "cumplimiento_sla_pct": round(cumplimiento_sla, 2),
                "throughput_diario_unidades": round(throughput, 2),
                "score_riesgo": round(riesgo, 2),
                "score_readiness": round(readiness, 2),
            }
        )

    scenario_df = pd.DataFrame(out_rows)
    scenario_df.to_csv(DATA_PROCESSED_DIR / "scenario_resultados.csv", index=False)

    # Visual comparativa
    plt.figure(figsize=(11, 5))
    sns.lineplot(data=scenario_df, x="mix_ev_pct", y="throughput_diario_unidades", marker="o", label="Throughput")
    sns.lineplot(data=scenario_df, x="mix_ev_pct", y="cumplimiento_sla_pct", marker="o", label="Cumplimiento SLA")
    plt.title("Sensibilidad operativa ante transición EV")
    plt.xlabel("Mix EV (%)")
    plt.ylabel("Nivel (escala original)")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "escenarios_ev_sensibilidad.png", dpi=150)
    plt.close()

    recomendaciones = []
    for row in scenario_df.itertuples(index=False):
        if row.score_riesgo > 72 and row.mix_ev_pct >= 70:
            recomendaciones.append(
                f"- {row.escenario}: activar incremento de capacidad de carga y políticas de pre-secuenciación EV para contener cola (> {row.espera_carga_min} min)."
            )
        elif row.cumplimiento_sla_pct < 80:
            recomendaciones.append(
                f"- {row.escenario}: reforzar ventanas de expedición y buffering logístico para evitar caída de SLA ({row.cumplimiento_sla_pct}%)."
            )
        else:
            recomendaciones.append(
                f"- {row.escenario}: operación viable con control de ocupación de patio ({row.ocupacion_patio_pct}%)."
            )

    (OUTPUT_REPORTS_DIR / "recomendaciones_escenarios.md").write_text(
        "# Recomendaciones por escenario\n\n" + "\n".join(recomendaciones),
        encoding="utf-8",
    )

    return scenario_df


if __name__ == "__main__":
    run_scenario_engine()
