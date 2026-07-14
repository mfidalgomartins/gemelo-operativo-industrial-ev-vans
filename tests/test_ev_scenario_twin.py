from __future__ import annotations

import pandas as pd
import pytest

from gemelo_operativo_ev import ev_scenario_twin as scenario_twin
from gemelo_operativo_ev.ev_scenario_twin import _base_metrics, _simulate


def _make_base() -> dict[str, float]:
    return {
        "throughput": 50.0,
        "tiempo_total_interno": 300.0,
        "ocupacion_media_patio": 0.65,
        "ocupacion_pico_patio": 0.90,
        "espera_carga": 45.0,
        "riesgo_salida_baja_readiness": 0.12,
        "riesgo_congestion": 0.18,
        "vehiculos_retrasados": 0.20,
        "estabilidad_operativa": 72.0,
        "share_ev": 0.40,
    }


def _neutral_params() -> dict[str, float]:
    return {
        "share_ev_delta": 0.0,
        "sequencing_gain": 0.0,
        "charging_gain": 0.0,
        "yard_gain": 0.0,
        "dispatch_pressure": 0.0,
        "shift_loss": 0.0,
    }


def test_simulate_neutral_params_returns_base_throughput() -> None:
    base = _make_base()
    result = _simulate(base, _neutral_params())
    assert abs(result["throughput"] - base["throughput"]) < 1e-6


def test_simulate_charging_gain_reduces_wait_time() -> None:
    base = _make_base()
    params_with_gain = {**_neutral_params(), "charging_gain": 0.5}
    result = _simulate(base, params_with_gain)
    assert result["espera_carga"] < base["espera_carga"]


def test_simulate_ev_delta_increases_yard_occupancy() -> None:
    base = _make_base()
    params_with_ev = {**_neutral_params(), "share_ev_delta": 0.3}
    result = _simulate(base, params_with_ev)
    assert result["ocupacion_media_patio"] > base["ocupacion_media_patio"]


def test_simulate_dispatch_pressure_raises_exit_risk() -> None:
    base = _make_base()
    params_pressure = {**_neutral_params(), "dispatch_pressure": 0.5}
    result = _simulate(base, params_pressure)
    assert result["riesgo_salida_baja_readiness"] > base["riesgo_salida_baja_readiness"]


def test_simulate_shift_loss_reduces_throughput() -> None:
    base = _make_base()
    params_shift = {**_neutral_params(), "shift_loss": 0.5}
    result = _simulate(base, params_shift)
    assert result["throughput"] < base["throughput"]


def test_simulate_yard_gain_reduces_congestion_risk() -> None:
    base = _make_base()
    params_yard = {**_neutral_params(), "yard_gain": 0.5}
    result = _simulate(base, params_yard)
    assert result["riesgo_congestion"] < base["riesgo_congestion"]


def test_simulate_combined_improvement_better_than_base() -> None:
    """Scenario 8: all gains should improve stability vs base."""
    base = _make_base()
    params_combined = {
        "share_ev_delta": 0.25,
        "sequencing_gain": 0.35,
        "charging_gain": 0.30,
        "yard_gain": 0.30,
        "dispatch_pressure": 0.05,
        "shift_loss": 0.00,
    }
    params_base_ev = {
        "share_ev_delta": 0.10,
        "sequencing_gain": 0.05,
        "charging_gain": 0.00,
        "yard_gain": 0.00,
        "dispatch_pressure": 0.00,
        "shift_loss": 0.00,
    }
    combined = _simulate(base, params_combined)
    base_ev = _simulate(base, params_base_ev)

    assert combined["estabilidad_operativa"] > base_ev["estabilidad_operativa"]


def test_simulate_output_keys_complete() -> None:
    base = _make_base()
    result = _simulate(base, _neutral_params())

    expected_keys = {
        "throughput",
        "tiempo_total_interno",
        "ocupacion_media_patio",
        "ocupacion_pico_patio",
        "espera_carga",
        "riesgo_salida_baja_readiness",
        "riesgo_congestion",
        "vehiculos_retrasados",
        "estabilidad_operativa",
    }
    assert expected_keys.issubset(set(result.keys()))


def test_simulate_risk_values_clamped_to_unit_interval() -> None:
    base = _make_base()
    extreme_params = {
        "share_ev_delta": 1.0,
        "sequencing_gain": 0.0,
        "charging_gain": 0.0,
        "yard_gain": 0.0,
        "dispatch_pressure": 1.0,
        "shift_loss": 1.0,
    }
    result = _simulate(base, extreme_params)

    assert 0 <= result["riesgo_salida_baja_readiness"] <= 1
    assert 0 <= result["riesgo_congestion"] <= 1
    assert 0 <= result["vehiculos_retrasados"] <= 1
    assert 0 <= result["estabilidad_operativa"] <= 100


@pytest.mark.parametrize(
    "key, value",
    [
        ("share_ev_delta", -0.1),
        ("sequencing_gain", 1.1),
        ("charging_gain", float("nan")),
    ],
)
def test_simulate_rejects_invalid_parameters(key: str, value: float) -> None:
    params = {**_neutral_params(), key: value}

    with pytest.raises(ValueError, match="no finitos|fuera del intervalo"):
        _simulate(_make_base(), params)


def test_simulate_rejects_invalid_base_metric() -> None:
    base = {**_make_base(), "share_ev": 1.2}

    with pytest.raises(ValueError, match="Métricas base fuera de rango"):
        _simulate(base, _neutral_params())


def test_simulate_rejects_non_numeric_input() -> None:
    params = {**_neutral_params(), "yard_gain": "alto"}

    with pytest.raises(TypeError, match="Inputs de escenario no numéricos"):
        _simulate(_make_base(), params)  # type: ignore[arg-type]


def test_base_metrics_measures_observed_readiness_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    tables = {
        "vehicle_readiness_features": pd.DataFrame(
            {"fecha_real": ["2025-01-01"] * 4, "total_internal_lead_time": [100, 110, 120, 130]}
        ),
        "yard_features": pd.DataFrame({"yard_occupancy_rate": [0.4, 0.6], "yard_saturation_score": [20.0, 80.0]}),
        "charging_features": pd.DataFrame({"avg_wait_to_charge": [20.0, 40.0]}),
        "vw_dispatch_readiness": pd.DataFrame(
            {
                "readiness_final_flag": [True, True, True, False],
                "departed_flag": [True, True, True, False],
                "delayed_flag": [False, True, False, False],
            }
        ),
        "launch_transition_features": pd.DataFrame({"share_ev": [0.4, 0.5]}),
    }

    monkeypatch.setattr(scenario_twin, "_read", lambda name, parse_dates=None: tables[name])

    metrics = _base_metrics()

    assert metrics["riesgo_salida_baja_readiness"] == pytest.approx(0.25)
