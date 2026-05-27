from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ev_feature_engineering import (
    _build_area_shift_features,
    _build_charging_features,
    _build_vehicle_readiness_features,
    _build_yard_features,
)


def _make_vehicle_flow() -> pd.DataFrame:
    n = 50
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "orden_id": [f"O{i:04d}" for i in range(n)],
            "vehiculo_id": [f"V{i:04d}" for i in range(n)],
            "version_id": [f"VER{i % 3:02d}" for i in range(n)],
            "tipo_propulsion": np.where(rng.random(n) > 0.4, "EV", "ICE"),
            "turno": np.random.choice(["A", "B", "C"], n),
            "fecha_real": pd.date_range("2025-01-01", periods=n, freq="8h"),
            "planned_to_actual_sequence_gap": rng.normal(0, 5, n),
            "total_internal_lead_time_min": rng.uniform(120, 480, n),
            "yard_wait_time_min": rng.uniform(10, 240, n),
            "charging_wait_time_min": rng.uniform(0, 180, n),
            "charging_duration_min": rng.uniform(30, 300, n),
            "soc_gap_before_dispatch": rng.uniform(0, 40, n),
            "dispatch_delay_min": rng.uniform(0, 120, n),
            "non_productive_moves_count": rng.integers(0, 5, n),
            "blocking_exposure": rng.uniform(0, 1, n),
            "complejidad_montaje": rng.uniform(1, 5, n),
            "timestamp_fin_linea": pd.date_range("2025-01-01", periods=n, freq="8h"),
            "timestamp_entrada_patio": pd.date_range("2025-01-01 01:00", periods=n, freq="8h"),
            "timestamp_inicio_carga": pd.date_range("2025-01-01 02:00", periods=n, freq="8h"),
            "timestamp_fin_carga": pd.date_range("2025-01-01 03:00", periods=n, freq="8h"),
            "timestamp_salida": pd.date_range("2025-01-01 04:00", periods=n, freq="8h"),
        }
    )


def _make_area_shift() -> pd.DataFrame:
    n = 60
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "fecha": pd.date_range("2025-01-01", periods=n, freq="D"),
            "turno": np.random.choice(["A", "B", "C"], n),
            "area": np.random.choice(["SECUENCIACION", "PATIO", "CARGA", "EXPEDICION"], n),
            "throughput_gap": rng.normal(0, 3, n),
            "congestion_index": rng.uniform(0, 100, n),
            "avg_wait_time": rng.uniform(5, 120, n),
            "queue_pressure_score": rng.uniform(0, 100, n),
            "slot_utilization": rng.uniform(0, 1.2, n),
            "yard_occupancy_rate": rng.uniform(0, 1, n),
            "bottleneck_density": rng.uniform(0, 1, n),
            "dispatch_risk_density": rng.uniform(0, 1, n),
            "operational_stress_score": rng.uniform(0, 100, n),
        }
    )


def _make_charging() -> pd.DataFrame:
    n = 80
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "fecha": pd.date_range("2025-01-01", periods=n, freq="6h"),
            "turno": np.random.choice(["A", "B", "C"], n),
            "zona_carga": np.random.choice(["Z1", "Z2", "Z3"], n),
            "slot_id": [f"S{i % 10:02d}" for i in range(n)],
            "sessions_count": rng.integers(1, 20, n),
            "avg_wait_time_min": rng.uniform(0, 120, n),
            "energy_delivered_kwh": rng.uniform(10, 80, n),
            "interruption_rate": rng.uniform(0, 0.3, n),
            "slot_utilization_ratio": rng.uniform(0, 1.5, n),
            "avg_soc_gap_pct": rng.uniform(-5, 15, n),
        }
    )


def _make_yard() -> pd.DataFrame:
    n = 100
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "ts_hour": pd.date_range("2025-01-01", periods=n, freq="h"),
            "zona_patio": np.random.choice(["NORTE", "SUR", "CENTRAL", "CARGA"], n),
            "avg_dwell_time_min": rng.uniform(10, 300, n),
            "p95_dwell_time_min": rng.uniform(60, 600, n),
            "blocking_rate": rng.uniform(0, 0.5, n),
            "movement_density": rng.uniform(0, 10, n),
            "non_productive_move_rate": rng.uniform(0, 0.4, n),
            "operational_risk_score": rng.uniform(0, 100, n),
            "yard_occupancy_rate": rng.uniform(0, 1, n),
        }
    )


def test_build_vehicle_readiness_features_shape_and_score_range() -> None:
    vf = _make_vehicle_flow()
    result = _build_vehicle_readiness_features(vf)

    assert len(result) == len(vf)
    assert "readiness_risk_score_input" in result.columns
    assert "version_complexity_score" in result.columns
    assert result["readiness_risk_score_input"].between(0, 100).all()
    assert result["version_complexity_score"].ge(0).all()


def test_build_vehicle_readiness_features_expected_columns() -> None:
    vf = _make_vehicle_flow()
    result = _build_vehicle_readiness_features(vf)

    expected = {
        "orden_id", "vehiculo_id", "version_id", "tipo_propulsion", "turno",
        "fecha_real", "planned_to_actual_sequence_gap", "total_internal_lead_time",
        "yard_wait_time", "charging_wait_time", "charging_duration",
        "soc_gap_before_dispatch", "dispatch_delay_min", "non_productive_moves_count",
        "blocking_exposure", "version_complexity_score", "readiness_risk_score_input",
    }
    assert expected.issubset(set(result.columns))
    assert "total_internal_lead_time_min" not in result.columns


def test_build_area_shift_features_passthrough() -> None:
    area = _make_area_shift()
    result = _build_area_shift_features(area)

    assert len(result) == len(area)
    assert "operational_stress_score" in result.columns
    assert "throughput_gap" in result.columns


def test_build_charging_features_score_range() -> None:
    ch = _make_charging()
    result = _build_charging_features(ch)

    assert "charger_pressure_score" in result.columns
    assert result["charger_pressure_score"].between(0, 100).all()
    assert "target_soc_miss_rate" in result.columns
    assert result["target_soc_miss_rate"].between(0, 1).all()


def test_build_yard_features_rename() -> None:
    yard = _make_yard()
    result = _build_yard_features(yard)

    assert "timestamp" in result.columns
    assert "ts_hour" not in result.columns
    assert "avg_dwell_time" in result.columns
    assert "yard_saturation_score" in result.columns
