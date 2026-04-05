from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR, PROJECT_ROOT


EV_DIR = DATA_PROCESSED_DIR / "ev_factory"


@dataclass
class FeatureBuildResult:
    tables: Dict[str, int]


def _read_csv(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = EV_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe tabla requerida para features: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def _build_vehicle_readiness_features(vf: pd.DataFrame) -> pd.DataFrame:
    out = vf.copy()
    out["version_complexity_score"] = out["complejidad_montaje"].clip(lower=0)
    out["readiness_risk_score_input"] = (
        0.20 * out["planned_to_actual_sequence_gap"].abs().clip(upper=25) / 25
        + 0.18 * out["yard_wait_time_min"].clip(lower=0, upper=360) / 360
        + 0.18 * out["charging_wait_time_min"].clip(lower=0, upper=360) / 360
        + 0.14 * out["charging_duration_min"].clip(lower=0, upper=300) / 300
        + 0.12 * out["soc_gap_before_dispatch"].clip(lower=0, upper=40) / 40
        + 0.10 * out["dispatch_delay_min"].clip(lower=0, upper=360) / 360
        + 0.08 * out["blocking_exposure"].clip(lower=0, upper=1)
    ) * 100

    cols = [
        "orden_id",
        "vehiculo_id",
        "version_id",
        "tipo_propulsion",
        "turno",
        "fecha_real",
        "planned_to_actual_sequence_gap",
        "total_internal_lead_time_min",
        "yard_wait_time_min",
        "charging_wait_time_min",
        "charging_duration_min",
        "soc_gap_before_dispatch",
        "dispatch_delay_min",
        "non_productive_moves_count",
        "blocking_exposure",
        "version_complexity_score",
        "readiness_risk_score_input",
    ]
    return out[cols].rename(
        columns={
            "total_internal_lead_time_min": "total_internal_lead_time",
            "yard_wait_time_min": "yard_wait_time",
            "charging_wait_time_min": "charging_wait_time",
            "charging_duration_min": "charging_duration",
        }
    )


def _build_area_shift_features(area_shift: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "fecha",
        "turno",
        "area",
        "throughput_gap",
        "congestion_index",
        "avg_wait_time",
        "queue_pressure_score",
        "slot_utilization",
        "yard_occupancy_rate",
        "bottleneck_density",
        "dispatch_risk_density",
        "operational_stress_score",
    ]
    out = area_shift[cols].copy()
    return out


def _build_charging_features(ch: pd.DataFrame) -> pd.DataFrame:
    ch2 = ch.copy()
    ch2["target_soc_miss_rate"] = (ch2["avg_soc_gap_pct"].fillna(0) > 5).astype(float)
    ch2["charger_pressure_score"] = (
        0.35 * ch2["slot_utilization_ratio"].clip(lower=0, upper=1.5) / 1.5
        + 0.30 * ch2["avg_wait_time_min"].clip(lower=0, upper=240) / 240
        + 0.20 * ch2["interruption_rate"].clip(lower=0, upper=1)
        + 0.15 * ch2["target_soc_miss_rate"].clip(lower=0, upper=1)
    ) * 100

    out = (
        ch2.groupby(["fecha", "turno", "zona_carga", "slot_id"], as_index=False)
        .agg(
            sessions_per_shift=("sessions_count", "sum"),
            avg_wait_to_charge=("avg_wait_time_min", "mean"),
            avg_energy_delivered=("energy_delivered_kwh", "mean"),
            interruption_rate=("interruption_rate", "mean"),
            target_soc_miss_rate=("target_soc_miss_rate", "mean"),
            charger_pressure_score=("charger_pressure_score", "mean"),
        )
    )
    return out


def _build_yard_features(yard: pd.DataFrame) -> pd.DataFrame:
    out = (
        yard.groupby(["ts_hour", "zona_patio"], as_index=False)
        .agg(
            avg_dwell_time=("avg_dwell_time_min", "mean"),
            p95_dwell_time=("p95_dwell_time_min", "mean"),
            blocking_rate=("blocking_rate", "mean"),
            movement_density=("movement_density", "mean"),
            non_productive_move_rate=("non_productive_move_rate", "mean"),
            yard_saturation_score=("operational_risk_score", "mean"),
            yard_occupancy_rate=("yard_occupancy_rate", "mean"),
        )
    )
    out = out.rename(columns={"ts_hour": "timestamp"})
    return out


def _build_launch_transition_features(
    vehicle_features: pd.DataFrame,
    charging_features: pd.DataFrame,
    yard_features: pd.DataFrame,
    dispatch: pd.DataFrame,
) -> pd.DataFrame:
    vf = vehicle_features.copy()
    vf["fecha_real"] = pd.to_datetime(vf["fecha_real"], errors="coerce")
    vf["week"] = vf["fecha_real"].dt.to_period("W").dt.start_time

    ch = charging_features.copy()
    ch["fecha"] = pd.to_datetime(ch["fecha"], errors="coerce")
    ch["week"] = ch["fecha"].dt.to_period("W").dt.start_time

    yd = yard_features.copy()
    yd["timestamp"] = pd.to_datetime(yd["timestamp"], errors="coerce")
    yd["week"] = yd["timestamp"].dt.to_period("W").dt.start_time

    dr = dispatch.copy()
    dr["fecha"] = pd.to_datetime(dr["fecha"], errors="coerce")
    dr["week"] = dr["fecha"].dt.to_period("W").dt.start_time

    weekly = (
        vf.groupby("week", as_index=False)
        .agg(
            share_ev=("tipo_propulsion", lambda s: float((s == "EV").mean())),
            readiness_gap_trend=("readiness_risk_score_input", "mean"),
            total_internal_lead_time=("total_internal_lead_time", "mean"),
        )
        .merge(
            ch.groupby("week", as_index=False).agg(
                charging_capacity_gap=("charger_pressure_score", "mean"),
                ev_operational_load_index=("sessions_per_shift", "mean"),
            ),
            on="week",
            how="left",
        )
        .merge(
            yd.groupby("week", as_index=False).agg(
                yard_transition_stress_index=("yard_saturation_score", "mean"),
            ),
            on="week",
            how="left",
        )
        .merge(
            dr.groupby("week", as_index=False).agg(
                dispatch_stability_index=(
                    "dispatch_readiness_risk_score",
                    lambda s: float(100 - np.clip(s.mean(), 0, 100)),
                )
            ),
            on="week",
            how="left",
        )
    )

    weekly["launch_transition_features_id"] = [f"LTF_{i:04d}" for i in range(1, len(weekly) + 1)]
    cols = [
        "launch_transition_features_id",
        "week",
        "share_ev",
        "ev_operational_load_index",
        "readiness_gap_trend",
        "charging_capacity_gap",
        "yard_transition_stress_index",
        "dispatch_stability_index",
    ]
    return weekly[cols]


def _write_feature_dictionary(path: Path) -> None:
    path.write_text(
        """# Feature Dictionary - Gemelo Operativo EV

## vehicle_readiness_features
- `planned_to_actual_sequence_gap` (derivada): desviación de secuencia real vs plan.
- `total_internal_lead_time` (observada/derivada): tiempo desde fin de línea a salida.
- `yard_wait_time` (observada): dwell medio en patio por vehículo.
- `charging_wait_time` (observada): espera media previa a carga.
- `charging_duration` (observada): duración de carga consolidada.
- `soc_gap_before_dispatch` (derivada): SOC objetivo menos SOC real de salida.
- `dispatch_delay_min` (observada): retraso operativo de salida.
- `non_productive_moves_count` (derivada): movimientos de patio no productivos.
- `blocking_exposure` (derivada): exposición a bloqueo en snapshots de patio.
- `version_complexity_score` (observada): complejidad de montaje de la versión.
- `readiness_risk_score_input` (derivada): score interpretable de riesgo previo al scoring final.

## area_shift_features
- `throughput_gap` (derivada): diferencia plan vs real por área-turno.
- `congestion_index` (derivada): presión de flujo en área.
- `avg_wait_time` (derivada): espera media operativa por área-turno.
- `queue_pressure_score` (derivada): presión de cola de carga.
- `slot_utilization` (derivada): utilización media de slots en turno.
- `yard_occupancy_rate` (derivada): ocupación del patio sobre capacidad estimada.
- `bottleneck_density` (derivada): intensidad de cuellos por área-turno.
- `dispatch_risk_density` (derivada): densidad de riesgo de expedición.
- `operational_stress_score` (derivada): score compuesto de estrés operativo.

## charging_features
- `sessions_per_shift` (observada agregada): volumen de sesiones por turno-slot.
- `avg_wait_to_charge` (derivada): espera media antes de carga.
- `avg_energy_delivered` (observada agregada): energía media entregada.
- `interruption_rate` (derivada): ratio de sesiones interrumpidas.
- `target_soc_miss_rate` (derivada): tasa de incumplimiento de objetivo SOC.
- `charger_pressure_score` (derivada): score compuesto de presión de carga.

## yard_features
- `avg_dwell_time` (observada agregada): dwell promedio por zona.
- `p95_dwell_time` (derivada): percentil 95 de dwell.
- `blocking_rate` (derivada): ratio de vehículos bloqueados.
- `movement_density` (observada agregada): intensidad de movimientos.
- `non_productive_move_rate` (derivada): peso de movimientos no productivos.
- `yard_saturation_score` (derivada): score de saturación operativa del patio.

## launch_transition_features
- `share_ev` (observada agregada): participación EV semanal.
- `ev_operational_load_index` (derivada): carga operativa asociada a EV.
- `readiness_gap_trend` (derivada): tendencia semanal del gap de readiness.
- `charging_capacity_gap` (derivada): gap presión/capacidad de carga.
- `yard_transition_stress_index` (derivada): estrés de transición en patio.
- `dispatch_stability_index` (derivada): estabilidad de expedición (100-riesgo).

## Valor operativo
Estas señales están diseñadas para decisiones interpretables de secuenciación, patio, carga y expedición durante un ramp-up EV sin depender de modelos de caja negra.
""",
        encoding="utf-8",
    )


def run_ev_feature_engineering() -> FeatureBuildResult:
    EV_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    vf = _read_csv(
        "vw_vehicle_flow_timeline",
        parse_dates=[
            "fecha_real",
            "timestamp_fin_linea",
            "timestamp_entrada_patio",
            "timestamp_inicio_carga",
            "timestamp_fin_carga",
            "timestamp_salida",
        ],
    )
    area_shift = _read_csv("mart_area_shift", parse_dates=["fecha"])
    charging = _read_csv("vw_charging_utilization", parse_dates=["fecha"])
    yard = _read_csv("vw_yard_congestion", parse_dates=["ts_hour"])
    dispatch = _read_csv("vw_dispatch_readiness", parse_dates=["fecha"])

    vehicle_features = _build_vehicle_readiness_features(vf)
    area_shift_features = _build_area_shift_features(area_shift)
    charging_features = _build_charging_features(charging)
    yard_features = _build_yard_features(yard)
    launch_features = _build_launch_transition_features(
        vehicle_features=vehicle_features,
        charging_features=charging_features,
        yard_features=yard_features,
        dispatch=dispatch,
    )

    outputs = {
        "vehicle_readiness_features": vehicle_features,
        "area_shift_features": area_shift_features,
        "charging_features": charging_features,
        "yard_features": yard_features,
        "launch_transition_features": launch_features,
    }

    counts: Dict[str, int] = {}
    for name, df in outputs.items():
        out_path = EV_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        counts[name] = int(df.shape[0])

    _write_feature_dictionary(PROJECT_ROOT / "docs" / "feature_dictionary.md")

    summary_lines = [
        "# Feature Engineering Summary",
        "",
        "## Tablas generadas",
    ]
    for name, n in counts.items():
        summary_lines.append(f"- {name}: {n} filas")

    summary_lines.extend(
        [
            "",
            "## Features clave",
            "- `readiness_risk_score_input`: base interpretable para scoring de readiness.",
            "- `operational_stress_score`: señal sintética de estrés área-turno.",
            "- `charger_pressure_score`: cuantifica presión real de carga.",
            "- `yard_saturation_score`: captura congestión estructural del patio.",
            "- `dispatch_stability_index`: estabilidad de salida en transición EV.",
        ]
    )

    (OUTPUT_REPORTS_DIR / "feature_engineering_summary.md").write_text("\n".join(summary_lines), encoding="utf-8")

    return FeatureBuildResult(tables=counts)


if __name__ == "__main__":
    result = run_ev_feature_engineering()
    print("Feature engineering EV completado")
    for table_name, rows in result.tables.items():
        print(f"- {table_name}: {rows}")
