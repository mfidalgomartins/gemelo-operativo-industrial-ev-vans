from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.synthetic_data_gen.config import SyntheticGenerationConfig
from src.synthetic_data_gen.utils import clamp, get_shift_start, ordered_phase, scenario_curve, shift_from_timestamp


@pytest.mark.parametrize(
    "kwargs, error_type, message",
    [
        ({"seed": "123"}, TypeError, "seed debe ser un entero"),
        ({"months": 8}, ValueError, "months debe estar entre 9 y 15"),
        ({"start_date": "no-es-fecha"}, ValueError, "start_date debe ser una fecha válida"),
        ({"shifts": ()}, ValueError, "shifts debe incluir al menos un turno"),
        ({"shifts": ("A", "D")}, ValueError, "shifts contiene turnos inválidos"),
    ],
)
def test_synthetic_generation_config_validation_errors(
    kwargs: dict[str, object], error_type: type[Exception], message: str
) -> None:
    cfg = SyntheticGenerationConfig(**kwargs)

    with pytest.raises(error_type, match=message):
        cfg.ensure_valid()


def test_shift_helpers_validate_boundaries_and_invalid_inputs() -> None:
    fecha = pd.Timestamp("2025-01-01 18:45")

    assert get_shift_start(fecha, "A") == pd.Timestamp("2025-01-01 06:00")
    assert get_shift_start(fecha, "C") == pd.Timestamp("2025-01-01 22:00")
    assert shift_from_timestamp(pd.Timestamp("2025-01-01 05:59")) == "C"
    assert shift_from_timestamp(pd.Timestamp("2025-01-01 06:00")) == "A"
    assert shift_from_timestamp(pd.Timestamp("2025-01-01 14:00")) == "B"
    assert shift_from_timestamp(pd.Timestamp("2025-01-01 22:00")) == "C"

    with pytest.raises(ValueError, match="turno inválido"):
        get_shift_start(fecha, "D")
    with pytest.raises(ValueError, match="ts no puede ser nulo"):
        shift_from_timestamp(pd.NaT)


def test_clamp_rejects_invalid_bounds() -> None:
    assert clamp(10, 0, 5) == 5.0

    with pytest.raises(ValueError, match="límite inferior no puede ser mayor"):
        clamp(1, 2, 1)


@pytest.mark.parametrize(
    "day_idx, total_days, expected",
    [
        (0, 10, "pre_lanzamiento"),
        (3, 10, "pre_serie"),
        (5, 10, "ramp_up"),
        (9, 10, "estable"),
        (0, 1, "pre_lanzamiento"),
    ],
)
def test_ordered_phase_boundaries(day_idx: int, total_days: int, expected: str) -> None:
    assert ordered_phase(day_idx, total_days) == expected


def test_scenario_curve_validates_phase_and_position() -> None:
    rng = np.random.default_rng(123)

    with pytest.raises(ValueError, match="position_in_phase debe estar entre 0 y 1"):
        scenario_curve("ramp_up", 1.2, rng)
    with pytest.raises(ValueError, match="fase inválida"):
        scenario_curve("desconocida", 0.5, rng)


def test_scenario_curve_outputs_are_clamped_and_complete() -> None:
    result = scenario_curve("estable", 1.0, np.random.default_rng(123))

    assert set(result) == {
        "share_ev",
        "intensidad_ramp_up",
        "disponibilidad_slots_carga",
        "presion_patio_indice",
        "restriccion_logistica_indice",
    }
    assert 0.02 <= result["share_ev"] <= 0.90
    assert 0.1 <= result["intensidad_ramp_up"] <= 1.0
    assert 0.62 <= result["disponibilidad_slots_carga"] <= 0.98
    assert 0.15 <= result["presion_patio_indice"] <= 1.0
    assert 0.05 <= result["restriccion_logistica_indice"] <= 1.0
