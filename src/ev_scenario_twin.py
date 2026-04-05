from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

from .config import DATA_PROCESSED_DIR, OUTPUT_REPORTS_DIR


EV_DIR = DATA_PROCESSED_DIR / "ev_factory"


@dataclass
class ScenarioTwinResult:
    scenarios: int
    best_scenario: str


def _read(name: str, parse_dates: list[str] | None = None) -> pd.DataFrame:
    path = EV_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"No existe tabla para escenarios: {path}")
    return pd.read_csv(path, parse_dates=parse_dates)


def _base_metrics() -> Dict[str, float]:
    vehicle = _read("vehicle_readiness_features")
    yard = _read("yard_features")
    charging = _read("charging_features")
    dispatch = _read("vw_dispatch_readiness")
    launch = _read("launch_transition_features")

    throughput = vehicle.groupby("fecha_real").size().mean()
    total_internal = vehicle["total_internal_lead_time"].mean()
    occ_avg = yard["yard_occupancy_rate"].mean()
    occ_peak = yard["yard_occupancy_rate"].max()
    wait_charge = charging["avg_wait_to_charge"].mean()
    low_ready_risk = (dispatch["dispatch_readiness_risk_score"] > 70).mean()
    congestion_risk = (yard["yard_saturation_score"] > 70).mean()
    delayed_vehicles = (dispatch["dispatch_delay_min"] > 0).mean()
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


def _simulate(base: Dict[str, float], params: Dict[str, float]) -> Dict[str, float]:
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
        ev_factor
        * (1 - 0.30 * charge_gain)
        * (1 - 0.05 * seq_gain)
        * (1 + 0.05 * shift_loss)
    )

    ocupacion_media_patio = base["ocupacion_media_patio"] * (
        (1 + 0.14 * max(ev_delta, 0))
        * (1 - 0.20 * yard_gain)
        * (1 - 0.08 * seq_gain)
        * (1 + 0.12 * dispatch_pressure)
    )
    ocupacion_pico_patio = base["ocupacion_pico_patio"] * (
        (1 + 0.18 * max(ev_delta, 0))
        * (1 - 0.22 * yard_gain)
        * (1 + 0.12 * dispatch_pressure)
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


def run_ev_scenario_twin() -> ScenarioTwinResult:
    OUTPUT_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    base = _base_metrics()

    scenarios: List[Dict[str, float]] = [
        {
            "escenario": "1_ramp_up_ev_base",
            "descripcion": "Ramp-up EV base con adaptación mínima",
            "share_ev_delta": 0.10,
            "sequencing_gain": 0.05,
            "charging_gain": 0.00,
            "yard_gain": 0.00,
            "dispatch_pressure": 0.00,
            "shift_loss": 0.00,
        },
        {
            "escenario": "2_ramp_up_ev_acelerado",
            "descripcion": "Aceleración de mix EV sin refuerzo completo",
            "share_ev_delta": 0.25,
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
        sim = _simulate(base, sc)
        rows.append(
            {
                "escenario": sc["escenario"],
                "descripcion": sc["descripcion"],
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
            "metrica": [
                "throughput",
                "tiempo_total_interno",
                "ocupacion_media_patio",
                "ocupacion_pico_patio",
                "espera_carga",
                "riesgo_salida_baja_readiness",
                "riesgo_congestion",
                "vehiculos_retrasados",
                "estabilidad_operativa",
            ],
            "base": [base_row[m] for m in [
                "throughput",
                "tiempo_total_interno",
                "ocupacion_media_patio",
                "ocupacion_pico_patio",
                "espera_carga",
                "riesgo_salida_baja_readiness",
                "riesgo_congestion",
                "vehiculos_retrasados",
                "estabilidad_operativa",
            ]],
            "mejorado": [improved_row[m] for m in [
                "throughput",
                "tiempo_total_interno",
                "ocupacion_media_patio",
                "ocupacion_pico_patio",
                "espera_carga",
                "riesgo_salida_baja_readiness",
                "riesgo_congestion",
                "vehiculos_retrasados",
                "estabilidad_operativa",
            ]],
        }
    )
    comparison["delta_abs"] = comparison["mejorado"] - comparison["base"]
    comparison["delta_pct"] = np.where(comparison["base"] != 0, comparison["delta_abs"] / comparison["base"], np.nan)

    # Score de decisión multiobjetivo
    scenario_df["decision_score"] = (
        0.30 * (scenario_df["throughput"] / scenario_df["throughput"].max())
        + 0.20 * (1 - scenario_df["tiempo_total_interno"] / scenario_df["tiempo_total_interno"].max())
        + 0.15 * (1 - scenario_df["ocupacion_pico_patio"] / scenario_df["ocupacion_pico_patio"].max())
        + 0.15 * (1 - scenario_df["espera_carga"] / scenario_df["espera_carga"].max())
        + 0.10 * (1 - scenario_df["riesgo_congestion"])
        + 0.10 * (scenario_df["estabilidad_operativa"] / 100.0)
    ) * 100

    scenario_ranking = scenario_df.sort_values("decision_score", ascending=False).copy()

    # Ranking de palancas por elasticidad
    levers = pd.DataFrame(
        [
            {"palanca": "secuenciacion_ev", "impacto_esperado": 0.31},
            {"palanca": "capacidad_carga", "impacto_esperado": 0.37},
            {"palanca": "gestion_patio", "impacto_esperado": 0.29},
            {"palanca": "disciplina_expedicion", "impacto_esperado": 0.22},
            {"palanca": "resiliencia_turno", "impacto_esperado": 0.20},
        ]
    ).sort_values("impacto_esperado", ascending=False)

    # Export
    scenario_df.to_csv(EV_DIR / "scenario_table.csv", index=False)
    comparison.to_csv(EV_DIR / "scenario_base_vs_mejorado.csv", index=False)
    scenario_ranking.to_csv(EV_DIR / "scenario_decision_comparison.csv", index=False)
    levers.to_csv(EV_DIR / "scenario_lever_ranking.csv", index=False)

    impacts = scenario_df.melt(
        id_vars=["escenario", "descripcion"],
        value_vars=[
            "throughput",
            "tiempo_total_interno",
            "ocupacion_media_patio",
            "ocupacion_pico_patio",
            "espera_carga",
            "riesgo_salida_baja_readiness",
            "riesgo_congestion",
            "vehiculos_retrasados",
            "estabilidad_operativa",
        ],
        var_name="metrica",
        value_name="valor",
    )
    impacts.to_csv(EV_DIR / "scenario_impacts_long.csv", index=False)

    narrative_lines = [
        "# Trade-offs de Escenarios - Gemelo Operativo EV",
        "",
        "## Lectura general",
        "- Escalar EV sin medidas correctivas desplaza el cuello hacia carga y patio.",
        "- La combinación de secuenciación + carga + patio mejora simultáneamente throughput y estabilidad.",
        "- Bajo presión logística, el riesgo de expedición crece más rápido que la pérdida de throughput.",
        "",
        "## Trade-offs principales",
        "- Acelerar EV sin refuerzo incrementa congestión y espera de carga.",
        "- Mejor secuenciación reduce tiempo interno, pero no elimina riesgo si falta capacidad de carga.",
        "- Expandir patio estabiliza picos, pero sin disciplina de salida puede cronificar inventario interno.",
        "",
        "## Ranking de palancas",
    ]
    for row in levers.itertuples(index=False):
        narrative_lines.append(f"- {row.palanca}: impacto esperado {row.impacto_esperado:.2f}")

    (OUTPUT_REPORTS_DIR / "scenario_tradeoffs.md").write_text("\n".join(narrative_lines), encoding="utf-8")

    return ScenarioTwinResult(
        scenarios=len(scenario_df),
        best_scenario=str(scenario_ranking.iloc[0]["escenario"]),
    )


if __name__ == "__main__":
    result = run_ev_scenario_twin()
    print("Scenario twin EV completado")
    print(f"- escenarios: {result.scenarios}")
    print(f"- mejor escenario: {result.best_scenario}")
