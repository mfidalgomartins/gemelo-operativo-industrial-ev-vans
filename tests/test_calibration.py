from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from gemelo_operativo_ev.calibration import (
    CALIBRATABLE_METRICS,
    CalibrationConfig,
    calibrate_scenario_coefficients,
    configured_calibration_path,
    load_approved_calibration,
    validate_calibration_frame,
)
from gemelo_operativo_ev.ev_scenario_twin import SCENARIO_PARAM_KEYS, _simulate


def _calibration_panel(seed: int = 17) -> tuple[pd.DataFrame, dict[str, float]]:
    rng = np.random.default_rng(seed)
    coefficients = {
        "share_ev_delta": -0.08,
        "sequencing_gain": 0.06,
        "charging_gain": 0.05,
        "yard_gain": 0.04,
        "dispatch_pressure": -0.07,
        "shift_loss": -0.09,
    }
    rows: list[dict[str, object]] = []
    units = [f"PLANT_{index}" for index in range(6)]
    periods = pd.date_range("2025-01-01", periods=12, freq="W")
    unit_effects = dict(zip(units, rng.normal(0, 0.015, len(units))))
    period_effects = dict(zip(periods, rng.normal(0, 0.01, len(periods))))
    for metric_index, metric in enumerate(CALIBRATABLE_METRICS):
        for unit in units:
            for period in periods:
                levers = {lever: float(rng.uniform(0, 1)) for lever in SCENARIO_PARAM_KEYS}
                log_effect = sum(coefficients[lever] * levers[lever] for lever in SCENARIO_PARAM_KEYS)
                log_effect += unit_effects[unit] + period_effects[period] + float(rng.normal(0, 0.002))
                baseline = 10.0 + metric_index
                rows.append(
                    {
                        "observation_id": f"{metric}-{unit}-{period.date()}",
                        "unit_id": unit,
                        "period": period.isoformat(),
                        "metric": metric,
                        "baseline_value": baseline,
                        "observed_value": baseline * np.exp(log_effect),
                        **levers,
                    }
                )
    return pd.DataFrame(rows), coefficients


def test_calibration_recovers_known_elasticities() -> None:
    frame, expected = _calibration_panel()
    result = calibrate_scenario_coefficients(
        frame,
        config=CalibrationConfig(min_observations=60, min_clusters=5),
    )
    assert result.metrics_estimated == len(CALIBRATABLE_METRICS)
    assert len(result.coefficients) == len(CALIBRATABLE_METRICS) * len(SCENARIO_PARAM_KEYS)
    assert result.coefficients["calibration_status"].eq("approved").all()
    estimates = result.coefficients.groupby("lever")["estimate"].mean().to_dict()
    for lever, expected_value in expected.items():
        assert estimates[lever] == pytest.approx(expected_value, abs=0.01)


def test_simulation_applies_approved_log_elasticities() -> None:
    base = {
        "throughput": 100.0,
        "tiempo_total_interno": 100.0,
        "ocupacion_media_patio": 0.5,
        "ocupacion_pico_patio": 0.7,
        "espera_carga": 30.0,
        "riesgo_salida_baja_readiness": 0.2,
        "riesgo_congestion": 0.1,
        "vehiculos_retrasados": 0.3,
        "estabilidad_operativa": 75.0,
        "share_ev": 0.4,
    }
    params = {lever: 0.0 for lever in SCENARIO_PARAM_KEYS}
    params["sequencing_gain"] = 0.5
    calibration = {
        metric: {lever: (0.1 if lever == "sequencing_gain" else 0.0) for lever in SCENARIO_PARAM_KEYS}
        for metric in CALIBRATABLE_METRICS
    }
    result = _simulate(base, params, calibration=calibration)
    assert result["throughput"] == pytest.approx(100 * np.exp(0.05))
    assert result["espera_carga"] == pytest.approx(30 * np.exp(0.05))


def test_calibration_rejects_absent_identifying_variation() -> None:
    frame, _ = _calibration_panel()
    frame["charging_gain"] = 0.0
    with pytest.raises(ValueError, match="sin variación identificadora"):
        calibrate_scenario_coefficients(frame)


def test_approved_calibration_round_trip(tmp_path, monkeypatch) -> None:
    frame, _ = _calibration_panel()
    result = calibrate_scenario_coefficients(frame)
    path = tmp_path / "coefficients.csv"
    result.coefficients.to_csv(path, index=False)

    loaded = load_approved_calibration(path)

    assert set(loaded) == set(CALIBRATABLE_METRICS)
    assert set(loaded["throughput"]) == set(SCENARIO_PARAM_KEYS)
    monkeypatch.setenv("EV_TWIN_CALIBRATION_FILE", str(path))
    assert configured_calibration_path() == path.resolve()
    monkeypatch.delenv("EV_TWIN_CALIBRATION_FILE")
    assert configured_calibration_path() is None


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda frame: frame.drop(columns=["unit_id"]), "faltan"),
        (lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True), "debe ser único"),
        (lambda frame: frame.assign(metric="unknown"), "Métricas no calibrables"),
        (lambda frame: frame.assign(observed_value=0), "estrictamente positivos"),
        (lambda frame: frame.assign(charging_gain=2), "permanecer en"),
    ],
)
def test_calibration_contract_rejects_invalid_frames(mutation, message: str) -> None:
    frame, _ = _calibration_panel()
    with pytest.raises(ValueError, match=message):
        validate_calibration_frame(mutation(frame))
