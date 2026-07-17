from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_AUDIT_DIR
from .utils import read_ev_csv, require_columns, write_text_utf8

EV_DIR = DATA_PROCESSED_DIR / "ev_factory"

VEHICLE_REQUIRED_COLUMNS = [
    "fecha_real",
    "tipo_propulsion",
    "turno",
    "planned_to_actual_sequence_gap",
    "version_complexity_score",
    "yard_wait_time",
    "blocking_exposure",
    "charging_wait_time",
    "soc_gap_before_dispatch",
    "dispatch_delay_min",
]

AREA_REQUIRED_COLUMNS = [
    "area",
    "dispatch_gap",
    "area_throughput_loss_proxy",
    "congestion_index",
    "avg_wait_time",
    "slot_utilization",
    "yard_occupancy_rate",
    "dispatch_risk_density",
    "operational_stress_score",
]

LAUNCH_REQUIRED_COLUMNS = ["week", "share_ev", "charging_capacity_gap", "yard_transition_stress_index"]

BOTTLENECK_LABELS = {
    "PRESION_CARGA": "presión de carga",
    "CONGESTION_PATIO": "congestión de patio",
    "RIESGO_EXPEDICION": "riesgo de expedición",
    "sin clasificar": "sin clasificar",
}


@dataclass
class DiagnosticResult:
    outputs: dict[str, int]


def _read(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    return read_ev_csv(name, EV_DIR, parse_dates=parse_dates)


def _score_to_100(series: pd.Series, upper: float) -> pd.Series:
    denominator = upper if np.isfinite(upper) and upper > 0 else 1e-9
    return np.clip(pd.to_numeric(series, errors="coerce").fillna(0) / denominator, 0, 1) * 100


def _initial_action(row: pd.Series) -> str:
    if row["charging_pressure_score"] >= 70:
        return "Reservar puntos de carga EV y ampliar ventana de pre-carga"
    if row["yard_congestion_score"] >= 70:
        return "Reducir permanencia y limpiar movimientos no productivos en patio"
    if row["sequence_disruption_score"] >= 65:
        return "Rebalancear secuencia y limitar clúster EV en turno"
    if row["dispatch_delay_risk_score"] >= 65:
        return "Priorizar expedición selectiva por preparación"
    return "Monitorizar y mantener configuración actual"


def run_ev_diagnostic_analysis() -> DiagnosticResult:
    OUTPUT_REPORTS_AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    vehicle = _read("vehicle_readiness_features", parse_dates=["fecha_real"])
    area = _read("area_shift_features", parse_dates=["fecha"])
    launch = _read("launch_transition_features", parse_dates=["week"])
    require_columns(vehicle, VEHICLE_REQUIRED_COLUMNS, "vehicle_readiness_features")
    require_columns(area, AREA_REQUIRED_COLUMNS, "area_shift_features")
    require_columns(launch, LAUNCH_REQUIRED_COLUMNS, "launch_transition_features")

    # Puntuaciones a nivel vehículo
    vehicle_diag = vehicle.copy()
    vehicle_diag["sequence_disruption_score"] = 0.6 * _score_to_100(
        vehicle_diag["planned_to_actual_sequence_gap"].abs(), upper=20
    ) + 0.4 * _score_to_100(vehicle_diag["version_complexity_score"], upper=5)
    vehicle_diag["yard_congestion_score"] = 0.55 * _score_to_100(
        vehicle_diag["yard_wait_time"], upper=240
    ) + 0.45 * _score_to_100(vehicle_diag["blocking_exposure"], upper=1)
    vehicle_diag["charging_pressure_score"] = 0.55 * _score_to_100(
        vehicle_diag["charging_wait_time"], upper=240
    ) + 0.45 * _score_to_100(vehicle_diag["soc_gap_before_dispatch"].clip(lower=0), upper=40)
    vehicle_diag["dispatch_delay_risk_score"] = 0.7 * _score_to_100(
        vehicle_diag["dispatch_delay_min"].clip(lower=0), upper=300
    ) + 0.3 * _score_to_100(vehicle_diag["soc_gap_before_dispatch"].clip(lower=0), upper=40)
    vehicle_diag["launch_transition_stress_score"] = (
        0.25 * vehicle_diag["sequence_disruption_score"]
        + 0.25 * vehicle_diag["yard_congestion_score"]
        + 0.30 * vehicle_diag["charging_pressure_score"]
        + 0.20 * vehicle_diag["dispatch_delay_risk_score"]
    )

    vehicle_diag["recommended_action_initial"] = np.select(
        [
            vehicle_diag["charging_pressure_score"] >= 70,
            vehicle_diag["yard_congestion_score"] >= 70,
            vehicle_diag["sequence_disruption_score"] >= 65,
            vehicle_diag["dispatch_delay_risk_score"] >= 65,
        ],
        [
            "Reservar puntos de carga EV y ampliar ventana de pre-carga",
            "Reducir permanencia y limpiar movimientos no productivos en patio",
            "Rebalancear secuencia y limitar clúster EV en turno",
            "Priorizar expedición selectiva por preparación",
        ],
        default="Monitorizar y mantener configuración actual",
    )

    # Área crítica y factor principal
    area_diag = area.copy()
    area_diag["area_criticality_score"] = (
        0.25 * _score_to_100(area_diag["area_throughput_loss_proxy"], upper=5)
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
            "Reservar puntos de carga EV y reforzar capacidad en horas punta",
            "Reducir permanencia y rediseñar pulmón por zona de patio",
            "Priorizar expedición selectiva y ventana de preparación",
        ],
        default="Monitorizar",
    )

    # Persistencia de cuellos: pico ocasional vs estructural
    area_persistence = (
        area_diag.assign(critical_flag=(area_diag["area_criticality_score"] >= 70).astype(int))
        .groupby("area", as_index=False)
        .agg(
            critical_share=("critical_flag", "mean"),
            avg_criticality=("area_criticality_score", "mean"),
            p95_criticality=("area_criticality_score", lambda s: float(np.quantile(s, 0.95))),
        )
    )
    area_persistence["tipo_cuello"] = np.where(
        area_persistence["critical_share"] >= 0.30,
        "ESTRUCTURAL",
        np.where(area_persistence["p95_criticality"] >= 80, "PICO_OCASIONAL", "ESTABLE"),
    )

    # Comparación EV vs no EV
    ev_compare = vehicle_diag.groupby("tipo_propulsion", as_index=False).agg(
        sequence_disruption_score=("sequence_disruption_score", "mean"),
        yard_congestion_score=("yard_congestion_score", "mean"),
        charging_pressure_score=("charging_pressure_score", "mean"),
        dispatch_delay_risk_score=("dispatch_delay_risk_score", "mean"),
        launch_transition_stress_score=("launch_transition_stress_score", "mean"),
    )

    shift_compare = vehicle_diag.groupby("turno", as_index=False).agg(
        sequence_disruption_score=("sequence_disruption_score", "mean"),
        yard_congestion_score=("yard_congestion_score", "mean"),
        charging_pressure_score=("charging_pressure_score", "mean"),
        dispatch_delay_risk_score=("dispatch_delay_risk_score", "mean"),
        launch_transition_stress_score=("launch_transition_stress_score", "mean"),
    )

    # Ranking de áreas
    area_ranking = (
        area_diag.groupby("area", as_index=False)
        .agg(
            area_criticality_score=("area_criticality_score", "mean"),
            dispatch_gap=("dispatch_gap", "mean"),
            area_throughput_loss_proxy=("area_throughput_loss_proxy", "mean"),
            avg_wait_time=("avg_wait_time", "mean"),
            main_bottleneck_driver=(
                "main_bottleneck_driver",
                lambda s: s.mode().iat[0] if not s.mode().empty else "sin clasificar",
            ),
            recommended_action_initial=(
                "recommended_action_initial",
                lambda s: s.mode().iat[0] if not s.mode().empty else "Sin acción definida",
            ),
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
        "diagnostic_area_persistence": area_persistence,
        "diagnostic_ev_vs_non_ev": ev_compare,
        "diagnostic_shift_comparison": shift_compare,
        "diagnostic_area_ranking": area_ranking,
    }

    out_counts: dict[str, int] = {}
    for name, df in outputs.items():
        df.to_csv(EV_DIR / f"{name}.csv", index=False)
        out_counts[name] = int(df.shape[0])

    # Resumen hallazgos
    top_areas_ranked = area_ranking.head(8)
    lines = [
        "# Diagnóstico Operativo - Hallazgos Priorizados",
        "",
        "## Principales áreas críticas",
    ]
    for row in top_areas_ranked.itertuples(index=False):
        lines.append(
            f"- {row.area}: puntuación={row.area_criticality_score:.1f}, "
            f"factor={BOTTLENECK_LABELS.get(row.main_bottleneck_driver, row.main_bottleneck_driver)}, "
            f"acción={row.recommended_action_initial}"
        )

    lines.extend(
        [
            "",
            "## Lecturas clave",
            f"- Diferencia EV vs no EV (tensión): {ev_compare.loc[ev_compare['tipo_propulsion'] == 'EV', 'launch_transition_stress_score'].mean() - ev_compare.loc[ev_compare['tipo_propulsion'] != 'EV', 'launch_transition_stress_score'].mean():.2f} puntos.",
            f"- Áreas clasificadas como estructurales: {int((area_persistence['tipo_cuello'] == 'ESTRUCTURAL').sum())}.",
        ]
    )
    write_text_utf8(OUTPUT_REPORTS_AUDIT_DIR / "diagnostic_findings.md", "\n".join(lines))

    return DiagnosticResult(outputs=out_counts)


if __name__ == "__main__":
    result = run_ev_diagnostic_analysis()
    print("Diagnóstico EV completado")
    for k, v in result.outputs.items():
        print(f"- {k}: {v}")
