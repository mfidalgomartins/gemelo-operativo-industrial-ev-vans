from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from numbers import Real
from pathlib import Path

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_AUDIT_DIR
from .utils import read_ev_csv, require_columns, write_text_utf8

EV_DIR = DATA_PROCESSED_DIR / "ev_factory"

BASE_METRIC_KEYS = [
    "throughput",
    "tiempo_total_interno",
    "ocupacion_media_patio",
    "ocupacion_pico_patio",
    "espera_carga",
    "riesgo_salida_baja_readiness",
    "riesgo_congestion",
    "vehiculos_retrasados",
    "estabilidad_operativa",
    "share_ev",
]

SCENARIO_PARAM_KEYS = [
    "share_ev_delta",
    "sequencing_gain",
    "charging_gain",
    "yard_gain",
    "dispatch_pressure",
    "shift_loss",
]

SCENARIO_METRICS = [
    "throughput",
    "tiempo_total_interno",
    "ocupacion_media_patio",
    "ocupacion_pico_patio",
    "espera_carga",
    "riesgo_salida_baja_readiness",
    "riesgo_congestion",
    "vehiculos_retrasados",
    "estabilidad_operativa",
]

CALIBRATABLE_SCENARIO_METRICS = tuple(metric for metric in SCENARIO_METRICS if metric != "estabilidad_operativa")

LEVER_LABELS = {
    "secuenciacion_ev": "secuenciación EV",
    "capacidad_carga": "capacidad de carga",
    "gestion_patio": "gestión de patio",
    "disciplina_expedicion": "disciplina de expedición",
    "resiliencia_turno": "resiliencia de turno",
}

LEVER_PRIORS = [
    {
        "palanca": "capacidad_carga",
        "impacto_esperado": 0.37,
        "clase_evidencia": "supuesto_parametrico_no_calibrado",
        "unidad": "indice_relativo_0_1",
    },
    {
        "palanca": "secuenciacion_ev",
        "impacto_esperado": 0.31,
        "clase_evidencia": "supuesto_parametrico_no_calibrado",
        "unidad": "indice_relativo_0_1",
    },
    {
        "palanca": "gestion_patio",
        "impacto_esperado": 0.29,
        "clase_evidencia": "supuesto_parametrico_no_calibrado",
        "unidad": "indice_relativo_0_1",
    },
    {
        "palanca": "disciplina_expedicion",
        "impacto_esperado": 0.22,
        "clase_evidencia": "supuesto_parametrico_no_calibrado",
        "unidad": "indice_relativo_0_1",
    },
    {
        "palanca": "resiliencia_turno",
        "impacto_esperado": 0.20,
        "clase_evidencia": "supuesto_parametrico_no_calibrado",
        "unidad": "indice_relativo_0_1",
    },
]


@dataclass
class ScenarioTwinResult:
    scenarios: int
    best_scenario: str


def _read(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    return read_ev_csv(name, EV_DIR, parse_dates=parse_dates, context="tabla para escenarios")


def _base_metrics() -> dict[str, float]:
    vehicle = _read("vehicle_readiness_features")
    yard = _read("yard_features")
    charging = _read("charging_features")
    dispatch = _read("vw_dispatch_readiness")
    launch = _read("launch_transition_features")
    require_columns(vehicle, ["fecha_real", "total_internal_lead_time"], "vehicle_readiness_features")
    require_columns(yard, ["yard_occupancy_rate", "yard_saturation_score"], "yard_features")
    require_columns(charging, ["avg_wait_to_charge"], "charging_features")
    require_columns(
        dispatch,
        ["readiness_final_flag", "departed_flag", "delayed_flag"],
        "vw_dispatch_readiness",
    )
    require_columns(launch, ["share_ev"], "launch_transition_features")

    throughput = vehicle.groupby("fecha_real").size().mean()
    total_internal = vehicle["total_internal_lead_time"].mean()
    occ_avg = yard["yard_occupancy_rate"].mean()
    occ_peak = yard["yard_occupancy_rate"].max()
    wait_charge = charging["avg_wait_to_charge"].mean()
    low_ready_risk = 1.0 - dispatch["readiness_final_flag"].astype(bool).mean()
    congestion_risk = (yard["yard_saturation_score"] > 70).mean()
    departed = dispatch.loc[dispatch["departed_flag"].astype(bool)]
    delayed_vehicles = departed["delayed_flag"].mean() if not departed.empty else 0.0
    stability = float(np.clip(100 - (40 * low_ready_risk + 35 * congestion_risk + 25 * delayed_vehicles), 0, 100))
    share_ev = launch["share_ev"].mean() if not launch.empty else 0.45

    return {
        "throughput": float(throughput),
        "tiempo_total_interno": float(total_internal),
        "ocupacion_media_patio": float(occ_avg),
        "ocupacion_pico_patio": float(occ_peak),
        "espera_carga": float(wait_charge),
        "riesgo_salida_baja_readiness": float(low_ready_risk),
        "riesgo_congestion": float(congestion_risk),
        "vehiculos_retrasados": float(delayed_vehicles),
        "estabilidad_operativa": float(stability),
        "share_ev": float(share_ev),
    }


def _simulate(
    base: dict[str, float],
    params: dict[str, float],
    *,
    calibration: Mapping[str, Mapping[str, float]] | None = None,
) -> dict[str, float]:
    missing_base = [key for key in BASE_METRIC_KEYS if key not in base]
    missing_params = [key for key in SCENARIO_PARAM_KEYS if key not in params]
    if missing_base or missing_params:
        raise ValueError(f"Inputs de escenario incompletos: base={missing_base}, params={missing_params}")

    invalid_types = [
        key
        for key in [*BASE_METRIC_KEYS, *SCENARIO_PARAM_KEYS]
        if isinstance((base | params)[key], bool) or not isinstance((base | params)[key], Real)
    ]
    if invalid_types:
        raise TypeError(f"Inputs de escenario no numéricos: {invalid_types}")

    non_finite = [key for key in BASE_METRIC_KEYS if not np.isfinite(base[key])]
    non_finite.extend(key for key in SCENARIO_PARAM_KEYS if not np.isfinite(params[key]))
    if non_finite:
        raise ValueError(f"Inputs de escenario no finitos: {sorted(set(non_finite))}")

    invalid_params = [key for key in SCENARIO_PARAM_KEYS if not 0 <= params[key] <= 1]
    if invalid_params:
        raise ValueError(f"Parámetros fuera del intervalo [0, 1]: {invalid_params}")

    non_negative_metrics = [
        "throughput",
        "tiempo_total_interno",
        "ocupacion_media_patio",
        "ocupacion_pico_patio",
        "espera_carga",
    ]
    invalid_metrics = [key for key in non_negative_metrics if base[key] < 0]
    invalid_metrics.extend(
        key
        for key in ["riesgo_salida_baja_readiness", "riesgo_congestion", "vehiculos_retrasados", "share_ev"]
        if not 0 <= base[key] <= 1
    )
    if not 0 <= base["estabilidad_operativa"] <= 100:
        invalid_metrics.append("estabilidad_operativa")
    if invalid_metrics:
        raise ValueError(f"Métricas base fuera de rango: {invalid_metrics}")

    if calibration is not None:
        missing_metrics = [metric for metric in CALIBRATABLE_SCENARIO_METRICS if metric not in calibration]
        missing_pairs = [
            f"{metric}.{lever}"
            for metric in CALIBRATABLE_SCENARIO_METRICS
            for lever in SCENARIO_PARAM_KEYS
            if metric in calibration and lever not in calibration[metric]
        ]
        if missing_metrics or missing_pairs:
            raise ValueError(f"Calibración incompleta: métricas={missing_metrics}, pares={missing_pairs}")

        simulated = {
            metric: float(
                base[metric]
                * np.exp(sum(float(calibration[metric][lever]) * params[lever] for lever in SCENARIO_PARAM_KEYS))
            )
            for metric in CALIBRATABLE_SCENARIO_METRICS
        }
        for metric in ["riesgo_salida_baja_readiness", "riesgo_congestion", "vehiculos_retrasados"]:
            simulated[metric] = float(np.clip(simulated[metric], 0, 1))
        simulated["estabilidad_operativa"] = float(
            np.clip(
                100
                - (
                    45 * simulated["riesgo_salida_baja_readiness"]
                    + 35 * simulated["riesgo_congestion"]
                    + 20 * simulated["vehiculos_retrasados"]
                    + 3
                    * max(
                        0,
                        (simulated["espera_carga"] - base["espera_carga"]) / max(base["espera_carga"], 1),
                    )
                ),
                0,
                100,
            )
        )
        return simulated

    ev_delta = params["share_ev_delta"]
    seq_gain = params["sequencing_gain"]
    charge_gain = params["charging_gain"]
    yard_gain = params["yard_gain"]
    dispatch_pressure = params["dispatch_pressure"]
    shift_loss = params["shift_loss"]

    ev_factor = 1 + max(ev_delta, 0) * 1.2

    throughput = base["throughput"] * (
        (1 - 0.10 * max(ev_delta, 0))
        * (1 + 0.06 * seq_gain)
        * (1 + 0.05 * charge_gain)
        * (1 + 0.04 * yard_gain)
        * (1 - 0.08 * dispatch_pressure)
        * (1 - 0.10 * shift_loss)
    )

    tiempo_total_interno = base["tiempo_total_interno"] * (
        (1 + 0.12 * max(ev_delta, 0))
        * (1 - 0.08 * seq_gain)
        * (1 - 0.10 * charge_gain)
        * (1 - 0.07 * yard_gain)
        * (1 + 0.06 * dispatch_pressure)
        * (1 + 0.07 * shift_loss)
    )

    espera_carga = base["espera_carga"] * (
        ev_factor * (1 - 0.30 * charge_gain) * (1 - 0.05 * seq_gain) * (1 + 0.05 * shift_loss)
    )

    ocupacion_media_patio = base["ocupacion_media_patio"] * (
        (1 + 0.14 * max(ev_delta, 0)) * (1 - 0.20 * yard_gain) * (1 - 0.08 * seq_gain) * (1 + 0.12 * dispatch_pressure)
    )
    ocupacion_pico_patio = base["ocupacion_pico_patio"] * (
        (1 + 0.18 * max(ev_delta, 0)) * (1 - 0.22 * yard_gain) * (1 + 0.12 * dispatch_pressure)
    )

    riesgo_salida = np.clip(
        base["riesgo_salida_baja_readiness"]
        * (
            (1 + 0.22 * max(ev_delta, 0))
            * (1 - 0.18 * charge_gain)
            * (1 - 0.10 * seq_gain)
            * (1 + 0.20 * dispatch_pressure)
        ),
        0,
        1,
    )

    riesgo_congestion = np.clip(
        base["riesgo_congestion"]
        * (
            (1 + 0.25 * max(ev_delta, 0))
            * (1 - 0.22 * yard_gain)
            * (1 - 0.12 * seq_gain)
            * (1 + 0.14 * dispatch_pressure)
            * (1 + 0.10 * shift_loss)
        ),
        0,
        1,
    )

    vehiculos_retrasados = np.clip(
        base["vehiculos_retrasados"]
        * (
            (1 + 0.18 * max(ev_delta, 0))
            * (1 - 0.12 * seq_gain)
            * (1 - 0.16 * charge_gain)
            * (1 - 0.10 * yard_gain)
            * (1 + 0.24 * dispatch_pressure)
            * (1 + 0.15 * shift_loss)
        ),
        0,
        1,
    )

    estabilidad = float(
        np.clip(
            100
            - (
                45 * riesgo_salida
                + 35 * riesgo_congestion
                + 20 * vehiculos_retrasados
                + 3 * max(0, (espera_carga - base["espera_carga"]) / max(base["espera_carga"], 1))
            ),
            0,
            100,
        )
    )

    return {
        "throughput": float(throughput),
        "tiempo_total_interno": float(tiempo_total_interno),
        "ocupacion_media_patio": float(ocupacion_media_patio),
        "ocupacion_pico_patio": float(ocupacion_pico_patio),
        "espera_carga": float(espera_carga),
        "riesgo_salida_baja_readiness": float(riesgo_salida),
        "riesgo_congestion": float(riesgo_congestion),
        "vehiculos_retrasados": float(vehiculos_retrasados),
        "estabilidad_operativa": estabilidad,
    }


def run_ev_scenario_twin(calibration_path: Path | None = None) -> ScenarioTwinResult:
    OUTPUT_REPORTS_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    base = _base_metrics()
    if calibration_path is None:
        from .calibration import configured_calibration_path

        calibration_path = configured_calibration_path()
    if calibration_path is not None:
        from .calibration import load_approved_calibration

        calibration = load_approved_calibration(calibration_path)
        evidence_class = "estimacion_calibrada"
    else:
        calibration = None
        evidence_class = "supuesto_parametrico_no_calibrado"

    scenarios: list[dict[str, float]] = [
        {
            "escenario": "1_ramp_up_ev_base",
            "descripcion": "Rampa EV objetivo sin medidas correctivas",
            "share_ev_delta": 0.25,
            "sequencing_gain": 0.00,
            "charging_gain": 0.00,
            "yard_gain": 0.00,
            "dispatch_pressure": 0.00,
            "shift_loss": 0.00,
        },
        {
            "escenario": "2_ramp_up_ev_acelerado",
            "descripcion": "Aceleración de cuota EV sin refuerzo completo",
            "share_ev_delta": 0.35,
            "sequencing_gain": 0.05,
            "charging_gain": 0.00,
            "yard_gain": 0.00,
            "dispatch_pressure": 0.05,
            "shift_loss": 0.00,
        },
        {
            "escenario": "3_aumento_slots_carga",
            "descripcion": "Incremento de disponibilidad de carga",
            "share_ev_delta": 0.18,
            "sequencing_gain": 0.05,
            "charging_gain": 0.35,
            "yard_gain": 0.00,
            "dispatch_pressure": 0.00,
            "shift_loss": 0.00,
        },
        {
            "escenario": "4_mejor_secuenciacion_ev",
            "descripcion": "Secuenciación EV más estable y balanceada",
            "share_ev_delta": 0.18,
            "sequencing_gain": 0.35,
            "charging_gain": 0.05,
            "yard_gain": 0.05,
            "dispatch_pressure": 0.00,
            "shift_loss": 0.00,
        },
        {
            "escenario": "5_expansion_o_mejor_uso_patio",
            "descripcion": "Expansión física o rediseño de uso de patio",
            "share_ev_delta": 0.18,
            "sequencing_gain": 0.05,
            "charging_gain": 0.00,
            "yard_gain": 0.35,
            "dispatch_pressure": 0.00,
            "shift_loss": 0.00,
        },
        {
            "escenario": "6_mas_presion_logistica_salida",
            "descripcion": "Mayor presión de expedición y ventanas tensas",
            "share_ev_delta": 0.18,
            "sequencing_gain": 0.05,
            "charging_gain": 0.00,
            "yard_gain": 0.00,
            "dispatch_pressure": 0.35,
            "shift_loss": 0.00,
        },
        {
            "escenario": "7_turno_tensionado_menor_disponibilidad",
            "descripcion": "Tensión en turno por menor disponibilidad operativa",
            "share_ev_delta": 0.18,
            "sequencing_gain": 0.00,
            "charging_gain": 0.00,
            "yard_gain": 0.00,
            "dispatch_pressure": 0.10,
            "shift_loss": 0.35,
        },
        {
            "escenario": "8_combinacion_medidas_correctivas",
            "descripcion": "Secuenciación + carga + patio con disciplina de salida",
            "share_ev_delta": 0.25,
            "sequencing_gain": 0.35,
            "charging_gain": 0.30,
            "yard_gain": 0.30,
            "dispatch_pressure": 0.05,
            "shift_loss": 0.00,
        },
    ]

    rows = []
    for sc in scenarios:
        sim = _simulate(base, sc, calibration=calibration)
        rows.append(
            {
                "escenario": sc["escenario"],
                "descripcion": sc["descripcion"],
                "clase_evidencia": evidence_class,
                "share_ev_estimado": min(1.0, base["share_ev"] + sc["share_ev_delta"]),
                **sim,
            }
        )

    scenario_df = pd.DataFrame(rows)
    scenario_df["throughput"] = scenario_df["throughput"].round(2)
    scenario_df["tiempo_total_interno"] = scenario_df["tiempo_total_interno"].round(2)
    scenario_df["ocupacion_media_patio"] = scenario_df["ocupacion_media_patio"].round(4)
    scenario_df["ocupacion_pico_patio"] = scenario_df["ocupacion_pico_patio"].round(4)
    scenario_df["espera_carga"] = scenario_df["espera_carga"].round(2)
    scenario_df["estabilidad_operativa"] = scenario_df["estabilidad_operativa"].round(2)

    base_row = scenario_df.loc[scenario_df["escenario"] == "1_ramp_up_ev_base"].iloc[0]
    improved_row = scenario_df.loc[scenario_df["escenario"] == "8_combinacion_medidas_correctivas"].iloc[0]

    comparison = pd.DataFrame(
        {
            "metrica": SCENARIO_METRICS,
            "base": [base_row[m] for m in SCENARIO_METRICS],
            "mejorado": [improved_row[m] for m in SCENARIO_METRICS],
        }
    )
    comparison["delta_abs"] = comparison["mejorado"] - comparison["base"]
    comparison["delta_pct"] = np.where(comparison["base"] != 0, comparison["delta_abs"] / comparison["base"], np.nan)

    # Puntuación de decisión multiobjetivo
    scenario_df["decision_score"] = (
        0.30 * (scenario_df["throughput"] / scenario_df["throughput"].max())
        + 0.20 * (1 - scenario_df["tiempo_total_interno"] / scenario_df["tiempo_total_interno"].max())
        + 0.15 * (1 - scenario_df["ocupacion_pico_patio"] / scenario_df["ocupacion_pico_patio"].max())
        + 0.15 * (1 - scenario_df["espera_carga"] / scenario_df["espera_carga"].max())
        + 0.10 * (1 - scenario_df["riesgo_congestion"])
        + 0.10 * (scenario_df["estabilidad_operativa"] / 100.0)
    ) * 100

    scenario_ranking = scenario_df.sort_values("decision_score", ascending=False).copy()

    # Priors de palancas para ordenar pilotos. No son elasticidades estimadas.
    if calibration is None:
        levers = pd.DataFrame(LEVER_PRIORS).sort_values("impacto_esperado", ascending=False)
    else:
        lever_impacts = {
            lever: float(np.mean([abs(calibration[metric][lever]) for metric in CALIBRATABLE_SCENARIO_METRICS]))
            for lever in SCENARIO_PARAM_KEYS
        }
        maximum_impact = max(lever_impacts.values()) or 1.0
        lever_name = {
            "share_ev_delta": "rampa_ev",
            "sequencing_gain": "secuenciacion_ev",
            "charging_gain": "capacidad_carga",
            "yard_gain": "gestion_patio",
            "dispatch_pressure": "disciplina_expedicion",
            "shift_loss": "resiliencia_turno",
        }
        levers = pd.DataFrame(
            [
                {
                    "palanca": lever_name[lever],
                    "impacto_esperado": impact / maximum_impact,
                    "clase_evidencia": evidence_class,
                    "unidad": "magnitud_elasticidad_log_relativa",
                }
                for lever, impact in lever_impacts.items()
            ]
        ).sort_values("impacto_esperado", ascending=False)

    # Export
    scenario_df.to_csv(EV_DIR / "scenario_table.csv", index=False)
    comparison.to_csv(EV_DIR / "scenario_base_vs_mejorado.csv", index=False)
    scenario_ranking.to_csv(EV_DIR / "scenario_decision_comparison.csv", index=False)
    levers.to_csv(EV_DIR / "scenario_lever_ranking.csv", index=False)

    impacts = scenario_df.melt(
        id_vars=["escenario", "descripcion"],
        value_vars=SCENARIO_METRICS,
        var_name="metrica",
        value_name="valor",
    )
    impacts.to_csv(EV_DIR / "scenario_impacts_long.csv", index=False)

    narrative_lines = [
        "# Compensaciones de Escenarios - Gemelo Operativo EV",
        "",
        "## Lectura general",
        "- Escalar EV sin medidas correctivas desplaza el cuello hacia carga y patio.",
        "- La combinación de secuenciación, carga y patio mejora caudal y estabilidad a la vez.",
        "- Bajo presión logística, el riesgo de expedición crece más rápido que la pérdida de caudal.",
        "",
        "## Compensaciones principales",
        "- Acelerar EV sin refuerzo incrementa congestión y espera de carga.",
        "- Mejor secuenciación reduce tiempo interno, pero no elimina riesgo si falta capacidad de carga.",
        "- Expandir patio estabiliza picos, pero sin disciplina de salida puede cronificar inventario interno.",
        "",
        f"## Ranking de palancas ({evidence_class.replace('_', ' ')})",
    ]
    for row in levers.itertuples(index=False):
        narrative_lines.append(
            f"- {LEVER_LABELS.get(row.palanca, row.palanca)}: impacto esperado {row.impacto_esperado:.2f}"
        )

    write_text_utf8(OUTPUT_REPORTS_AUDIT_DIR / "scenario_tradeoffs.md", "\n".join(narrative_lines))

    return ScenarioTwinResult(
        scenarios=len(scenario_df),
        best_scenario=str(scenario_ranking.iloc[0]["escenario"]),
    )


if __name__ == "__main__":
    result = run_ev_scenario_twin()
    print("Scenario twin EV completado")
    print(f"- escenarios: {result.scenarios}")
    print(f"- mejor escenario: {result.best_scenario}")
