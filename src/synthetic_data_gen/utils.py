from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


SHIFT_START_HOUR = {"A": 6, "B": 14, "C": 22}


def get_shift_start(fecha: pd.Timestamp, turno: str) -> pd.Timestamp:
    return fecha.normalize() + pd.Timedelta(hours=SHIFT_START_HOUR[turno])


def shift_from_timestamp(ts: pd.Timestamp) -> str:
    hour = ts.hour
    if 6 <= hour < 14:
        return "A"
    if 14 <= hour < 22:
        return "B"
    return "C"


def clamp(value: float, low: float, high: float) -> float:
    return float(np.clip(value, low, high))


def ordered_phase(day_idx: int, total_days: int) -> str:
    ratio = day_idx / max(total_days - 1, 1)
    if ratio < 0.20:
        return "pre_lanzamiento"
    if ratio < 0.40:
        return "pre_serie"
    if ratio < 0.75:
        return "ramp_up"
    return "estable"


def scenario_curve(phase: str, position_in_phase: float, rng: np.random.Generator) -> Dict[str, float]:
    jitter = lambda s=0.02: float(rng.normal(0, s))

    if phase == "pre_lanzamiento":
        share_ev = 0.04 + 0.08 * position_in_phase + jitter(0.01)
        intensidad = 0.25 + 0.25 * position_in_phase + jitter(0.03)
    elif phase == "pre_serie":
        share_ev = 0.12 + 0.16 * position_in_phase + jitter(0.015)
        intensidad = 0.45 + 0.20 * position_in_phase + jitter(0.03)
    elif phase == "ramp_up":
        share_ev = 0.28 + 0.36 * position_in_phase + jitter(0.02)
        intensidad = 0.65 + 0.30 * position_in_phase + jitter(0.04)
    else:
        share_ev = 0.64 + 0.12 * position_in_phase + jitter(0.02)
        intensidad = 0.58 + 0.10 * position_in_phase + jitter(0.03)

    disponibilidad_slots = clamp(0.95 - 0.18 * share_ev + jitter(0.02), 0.62, 0.98)
    presion_patio = clamp(0.30 + 0.75 * share_ev + 0.22 * intensidad + jitter(0.03), 0.15, 1.0)
    restriccion_logistica = clamp(0.20 + 0.50 * share_ev + jitter(0.04), 0.05, 1.0)

    return {
        "share_ev": clamp(share_ev, 0.02, 0.90),
        "intensidad_ramp_up": clamp(intensidad, 0.1, 1.0),
        "disponibilidad_slots_carga": disponibilidad_slots,
        "presion_patio_indice": presion_patio,
        "restriccion_logistica_indice": restriccion_logistica,
    }
