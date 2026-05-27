from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ev_scoring_framework import _map_action, _map_tier, _normalize_100, _normalize_percentile


def test_normalize_100_clamps_above_upper() -> None:
    series = pd.Series([0.0, 50.0, 100.0, 200.0])
    result = _normalize_100(series, upper=100.0)
    assert result.max() <= 100.0
    assert result.min() >= 0.0


def test_normalize_100_linearity_within_range() -> None:
    series = pd.Series([0.0, 50.0, 100.0])
    result = _normalize_100(series, upper=100.0)
    assert abs(result.iloc[0] - 0.0) < 1e-9
    assert abs(result.iloc[1] - 50.0) < 1e-9
    assert abs(result.iloc[2] - 100.0) < 1e-9


def test_normalize_percentile_output_range() -> None:
    rng = np.random.default_rng(42)
    series = pd.Series(rng.uniform(0, 500, 100))
    result = _normalize_percentile(series, q_hi=0.95)
    assert result.min() >= 0.0
    assert result.max() <= 100.0


def test_normalize_percentile_empty_series() -> None:
    series = pd.Series([], dtype=float)
    result = _normalize_percentile(series, q_hi=0.95)
    assert len(result) == 0


def test_normalize_percentile_all_zeros() -> None:
    series = pd.Series([0.0] * 10)
    result = _normalize_percentile(series, q_hi=0.95)
    assert (result == 0).all()


@pytest.mark.parametrize(
    "score, expected",
    [
        (90.0, "intervenir ahora"),
        (70.0, "estabilizar en la siguiente ola"),
        (55.0, "monitorizar muy de cerca"),
        (40.0, "mantener bajo observación"),
        (20.0, "sin prioridad inmediata"),
    ],
)
def test_map_tier_boundaries(score: float, expected: str) -> None:
    assert _map_tier(score) == expected


def test_map_tier_exact_boundaries() -> None:
    assert _map_tier(80.0) == "intervenir ahora"
    assert _map_tier(65.0) == "estabilizar en la siguiente ola"
    assert _map_tier(50.0) == "monitorizar muy de cerca"
    assert _map_tier(35.0) == "mantener bajo observación"
    assert _map_tier(34.9) == "sin prioridad inmediata"


def test_map_action_known_drivers() -> None:
    assert "carga" in _map_action("charging_risk_score")
    assert "patio" in _map_action("yard_risk_score")
    assert "expedición" in _map_action("dispatch_risk_score")
    assert "turnos" in _map_action("throughput_loss_score")
    assert "secuenciación" in _map_action("readiness_score")


def test_map_action_unknown_driver_returns_default() -> None:
    result = _map_action("nonexistent_driver")
    assert isinstance(result, str)
    assert len(result) > 0
