from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from .config import DATA_PROCESSED_DIR, OUTPUT_CHARTS_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


sns.set_theme(style="whitegrid")
EV_DIR = DATA_PROCESSED_DIR / "ev_factory"


@dataclass
class DiagnosticResult:
    outputs: Dict[str, int]


def _read(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = EV_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe tabla: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def _score_to_100(series: pd.Series, upper: float) -> pd.Series:
    return np.clip(series / upper, 0, 1) * 100


def _initial_action(row: pd.Series) -> str:
    if row["charging_pressure_score"] >= 70:
        return "Reservar slots de carga EV y ampliar ventana de pre-carga"
    if row["yard_congestion_score"] >= 70:
        return "Reducir dwell y limpiar movimientos no productivos en patio"
    if row["sequence_disruption_score"] >= 65:
        return "Rebalancear secuencia y limitar clúster EV en turno"
    if row["dispatch_delay_risk_score"] >= 65:
        return "Priorizar expedición selectiva por readiness"
    return "Monitorizar y mantener configuración actual"


def run_ev_diagnostic_analysis() -> DiagnosticResult:
    OUTPUT_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    vehicle = _read("vehicle_readiness_features", parse_dates=["fecha_real"])
    area = _read("area_shift_features", parse_dates=["fecha"])
    charging = _read("charging_features", parse_dates=["fecha"])
    yard = _read("yard_features", parse_dates=["timestamp"])
    launch = _read("launch_transition_features", parse_dates=["week"])
    bottleneck = _read("vw_shift_bottleneck_summary", parse_dates=["fecha"])

    # Scores a nivel vehículo
    vehicle_diag = vehicle.copy()
    vehicle_diag["sequence_disruption_score"] = (
        0.6 * _score_to_100(vehicle_diag["planned_to_actual_sequence_gap"].abs(), upper=20)
        + 0.4 * _score_to_100(vehicle_diag["version_complexity_score"], upper=5)
    )
    vehicle_diag["yard_congestion_score"] = (
        0.55 * _score_to_100(vehicle_diag["yard_wait_time"], upper=240)
        + 0.45 * _score_to_100(vehicle_diag["blocking_exposure"], upper=1)
    )
    vehicle_diag["charging_pressure_score"] = (
        0.55 * _score_to_100(vehicle_diag["charging_wait_time"], upper=240)
        + 0.45 * _score_to_100(vehicle_diag["soc_gap_before_dispatch"].clip(lower=0), upper=40)
    )
    vehicle_diag["dispatch_delay_risk_score"] = (
        0.7 * _score_to_100(vehicle_diag["dispatch_delay_min"].clip(lower=0), upper=300)
        + 0.3 * _score_to_100(vehicle_diag["soc_gap_before_dispatch"].clip(lower=0), upper=40)
    )
    vehicle_diag["launch_transition_stress_score"] = (
        0.25 * vehicle_diag["sequence_disruption_score"]
        + 0.25 * vehicle_diag["yard_congestion_score"]
        + 0.30 * vehicle_diag["charging_pressure_score"]
        + 0.20 * vehicle_diag["dispatch_delay_risk_score"]
    )

    vehicle_diag["recommended_action_initial"] = vehicle_diag.apply(_initial_action, axis=1)

    # Área crítica y driver principal
    area_diag = area.copy()
    area_diag["area_criticality_score"] = (
        0.25 * _score_to_100(area_diag["throughput_gap"].abs(), upper=25)
        + 0.20 * area_diag["congestion_index"].clip(0, 100)
        + 0.20 * _score_to_100(area_diag["avg_wait_time"], upper=240)
        + 0.15 * _score_to_100(area_diag["slot_utilization"], upper=1.2)
        + 0.20 * area_diag["operational_stress_score"].clip(0, 100)
    )

    conditions = [
        area_diag["slot_utilization"] >= area_diag[["yard_occupancy_rate", "dispatch_risk_density"]].max(axis=1),
        area_diag["yard_occupancy_rate"] >= area_diag[["slot_utilization", "dispatch_risk_density"]].max(axis=1),
    ]
    choices = ["PRESION_CARGA", "CONGESTION_PATIO"]
    area_diag["main_bottleneck_driver"] = np.select(conditions, choices, default="RIESGO_EXPEDICION")

    area_diag["recommended_action_initial"] = np.select(
        [
            area_diag["main_bottleneck_driver"] == "PRESION_CARGA",
            area_diag["main_bottleneck_driver"] == "CONGESTION_PATIO",
            area_diag["main_bottleneck_driver"] == "RIESGO_EXPEDICION",
        ],
        [
            "Reservar slots EV y reforzar capacidad en horas punta",
            "Reducir dwell y rediseñar buffer por zona de patio",
            "Priorizar expedición selectiva y ventana de readiness",
        ],
        default="Monitorizar",
    )

    # Persistencia de cuellos: pico ocasional vs estructural
    pers = (
        area_diag.assign(critical_flag=(area_diag["area_criticality_score"] >= 70).astype(int))
        .groupby("area", as_index=False)
        .agg(
            critical_share=("critical_flag", "mean"),
            avg_criticality=("area_criticality_score", "mean"),
            p95_criticality=("area_criticality_score", lambda s: float(np.quantile(s, 0.95))),
        )
    )
    pers["tipo_cuello"] = np.where(
        pers["critical_share"] >= 0.30,
        "ESTRUCTURAL",
        np.where(pers["p95_criticality"] >= 80, "PICO_OCASIONAL", "ESTABLE"),
    )

    # Comparación EV vs no EV
    ev_compare = (
        vehicle_diag.groupby("tipo_propulsion", as_index=False)
        .agg(
            sequence_disruption_score=("sequence_disruption_score", "mean"),
            yard_congestion_score=("yard_congestion_score", "mean"),
            charging_pressure_score=("charging_pressure_score", "mean"),
            dispatch_delay_risk_score=("dispatch_delay_risk_score", "mean"),
            launch_transition_stress_score=("launch_transition_stress_score", "mean"),
        )
    )

    shift_compare = (
        vehicle_diag.groupby("turno", as_index=False)
        .agg(
            sequence_disruption_score=("sequence_disruption_score", "mean"),
            yard_congestion_score=("yard_congestion_score", "mean"),
            charging_pressure_score=("charging_pressure_score", "mean"),
            dispatch_delay_risk_score=("dispatch_delay_risk_score", "mean"),
            launch_transition_stress_score=("launch_transition_stress_score", "mean"),
        )
    )

    # Ranking de áreas
    area_ranking = (
        area_diag.groupby("area", as_index=False)
        .agg(
            area_criticality_score=("area_criticality_score", "mean"),
            throughput_gap=("throughput_gap", "mean"),
            avg_wait_time=("avg_wait_time", "mean"),
            main_bottleneck_driver=("main_bottleneck_driver", lambda s: s.mode().iat[0] if not s.mode().empty else "N/A"),
            recommended_action_initial=("recommended_action_initial", lambda s: s.mode().iat[0] if not s.mode().empty else "N/A"),
        )
        .sort_values("area_criticality_score", ascending=False)
    )

    # Merge de contexto de transición
    launch_context = launch[["week", "share_ev", "charging_capacity_gap", "yard_transition_stress_index"]].copy()
    launch_context["week"] = pd.to_datetime(launch_context["week"], errors="coerce")
    vehicle_diag["week"] = vehicle_diag["fecha_real"].dt.to_period("W").dt.start_time
    vehicle_diag = vehicle_diag.merge(launch_context, on="week", how="left")

    # Export tablas
    outputs = {
        "diagnostic_vehicle_scores": vehicle_diag,
        "diagnostic_area_scores": area_diag,
        "diagnostic_area_persistence": pers,
        "diagnostic_ev_vs_non_ev": ev_compare,
        "diagnostic_shift_comparison": shift_compare,
        "diagnostic_area_ranking": area_ranking,
    }

    out_counts: Dict[str, int] = {}
    for name, df in outputs.items():
        df.to_csv(EV_DIR / f"{name}.csv", index=False)
        out_counts[name] = int(df.shape[0])

    # Gráficos explicativos
    plt.figure(figsize=(11, 5))
    sns.barplot(data=area_ranking.head(10), x="area", y="area_criticality_score", color="#b2182b")
    plt.title("Ranking de áreas críticas (score medio)")
    plt.xlabel("Área")
    plt.ylabel("Area Criticality Score")
    plt.xticks(rotation=25)
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "diagnostic_area_criticality_ranking.png", dpi=160)
    plt.close()

    plt.figure(figsize=(8, 5))
    evm = ev_compare.melt(id_vars=["tipo_propulsion"], var_name="score", value_name="value")
    sns.barplot(data=evm, x="score", y="value", hue="tipo_propulsion")
    plt.title("Comparación EV vs no EV en scores diagnósticos")
    plt.xlabel("Score")
    plt.ylabel("Valor")
    plt.xticks(rotation=25)
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "diagnostic_ev_vs_noev_scores.png", dpi=160)
    plt.close()

    plt.figure(figsize=(10, 5))
    sns.lineplot(data=shift_compare.melt(id_vars=["turno"], var_name="score", value_name="value"), x="turno", y="value", hue="score", marker="o")
    plt.title("Comparación por turno de presión operativa")
    plt.xlabel("Turno")
    plt.ylabel("Valor del score")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "diagnostic_shift_comparison.png", dpi=160)
    plt.close()

    plt.figure(figsize=(9, 5))
    sns.scatterplot(data=pers, x="critical_share", y="avg_criticality", hue="tipo_cuello", s=120)
    plt.title("Persistencia de cuellos: pico vs estructural")
    plt.xlabel("Share periodos críticos")
    plt.ylabel("Criticality media")
    plt.tight_layout()
    plt.savefig(OUTPUT_CHARTS_DIR / "diagnostic_persistence_map.png", dpi=160)
    plt.close()

    # Documento metodológico
    doc = PROJECT_ROOT / "docs" / "diagnostic_framework.md"
    doc.write_text(
        """# Diagnostic Framework - Gemelo Operativo EV

## Objetivo
Identificar dónde se rompe el flujo y priorizar acciones operativas interpretables en secuenciación, patio, carga y expedición.

## Capas de análisis
1. Secuenciación: desviación plan-real y complejidad de versión.
2. Patio: espera, bloqueo y no productividad de movimientos.
3. Carga: colas, presión de slot e incumplimiento de SOC objetivo.
4. Expedición: retrasos y gap de readiness.
5. Área-turno: estrés operacional y criticidad por impacto en throughput.

## Scores principales
- `sequence_disruption_score`
- `yard_congestion_score`
- `charging_pressure_score`
- `dispatch_delay_risk_score`
- `launch_transition_stress_score`
- `area_criticality_score`

## Lógica de persistencia
- `ESTRUCTURAL`: share de periodos críticos >= 30%.
- `PICO_OCASIONAL`: share crítico bajo, pero p95 de criticality muy alto.
- `ESTABLE`: sin evidencia de tensión sostenida.

## Salidas
- `diagnostic_vehicle_scores.csv`
- `diagnostic_area_scores.csv`
- `diagnostic_area_persistence.csv`
- `diagnostic_ev_vs_non_ev.csv`
- `diagnostic_shift_comparison.csv`
- `diagnostic_area_ranking.csv`

## Uso operativo
El framework conecta síntomas con acción inicial recomendada para facilitar priorización diaria y planificación semanal durante la transición EV.
""",
        encoding="utf-8",
    )

    # Resumen hallazgos
    top = area_ranking.head(8)
    lines = [
        "# Diagnóstico Operativo - Hallazgos Priorizados",
        "",
        "## Top áreas críticas",
    ]
    for row in top.itertuples(index=False):
        lines.append(
            f"- {row.area}: score={row.area_criticality_score:.1f}, driver={row.main_bottleneck_driver}, acción={row.recommended_action_initial}"
        )

    lines.extend(
        [
            "",
            "## Lecturas clave",
            f"- Diferencia EV vs no EV (stress): {ev_compare.loc[ev_compare['tipo_propulsion']=='EV','launch_transition_stress_score'].mean() - ev_compare.loc[ev_compare['tipo_propulsion']!='EV','launch_transition_stress_score'].mean():.2f} puntos.",
            f"- Áreas clasificadas como estructurales: {int((pers['tipo_cuello']=='ESTRUCTURAL').sum())}.",
        ]
    )
    (OUTPUT_REPORTS_DIR / "diagnostic_findings.md").write_text("\n".join(lines), encoding="utf-8")

    return DiagnosticResult(outputs=out_counts)


if __name__ == "__main__":
    result = run_ev_diagnostic_analysis()
    print("Diagnóstico EV completado")
    for k, v in result.outputs.items():
        print(f"- {k}: {v}")
