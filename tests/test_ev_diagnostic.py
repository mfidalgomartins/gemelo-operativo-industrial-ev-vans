from __future__ import annotations

import pandas as pd

from src.ev_diagnostic_analysis import _initial_action, _score_to_100


def test_score_to_100_clamps_output() -> None:
    series = pd.Series([-10.0, 0.0, 50.0, 100.0, 200.0])
    result = _score_to_100(series, upper=100.0)

    assert result.min() >= 0
    assert result.max() <= 100


def test_score_to_100_linearity() -> None:
    series = pd.Series([0.0, 50.0, 100.0])
    result = _score_to_100(series, upper=100.0)

    assert abs(result.iloc[0] - 0.0) < 1e-9
    assert abs(result.iloc[1] - 50.0) < 1e-9
    assert abs(result.iloc[2] - 100.0) < 1e-9


def test_initial_action_charging_takes_priority() -> None:
    row = pd.Series(
        {
            "charging_pressure_score": 75.0,
            "yard_congestion_score": 80.0,
            "sequence_disruption_score": 70.0,
            "dispatch_delay_risk_score": 70.0,
        }
    )
    action = _initial_action(row)
    assert "carga" in action.lower()


def test_initial_action_yard_second_priority() -> None:
    row = pd.Series(
        {
            "charging_pressure_score": 50.0,
            "yard_congestion_score": 75.0,
            "sequence_disruption_score": 70.0,
            "dispatch_delay_risk_score": 70.0,
        }
    )
    action = _initial_action(row)
    assert "dwell" in action.lower() or "patio" in action.lower()


def test_initial_action_default_when_all_below_threshold() -> None:
    row = pd.Series(
        {
            "charging_pressure_score": 30.0,
            "yard_congestion_score": 30.0,
            "sequence_disruption_score": 30.0,
            "dispatch_delay_risk_score": 30.0,
        }
    )
    action = _initial_action(row)
    assert "monitorizar" in action.lower()


def test_score_to_100_upper_zero_doesnt_divide_by_zero() -> None:
    series = pd.Series([0.0, 10.0])
    # upper=0 would cause division by zero — numpy clips to 0 naturally
    result = _score_to_100(series, upper=1e-9)
    assert result.notna().all()
