from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


VALID_SHIFTS = frozenset({"A", "B", "C"})


@dataclass(frozen=True)
class SyntheticGenerationConfig:
    seed: int = 20260328
    start_date: str = "2025-01-01"
    months: int = 12
    shifts: tuple[str, ...] = ("A", "B", "C")
    output_raw_dir: Path = Path("data/raw/ev_factory")
    output_report_dir: Path = Path("outputs/reports")

    def ensure_valid(self) -> None:
        if not isinstance(self.seed, int):
            raise TypeError("seed debe ser un entero.")
        if not isinstance(self.months, int):
            raise TypeError("months debe ser un entero.")
        if self.months < 9 or self.months > 15:
            raise ValueError("months debe estar entre 9 y 15 para cumplir requisitos.")
        try:
            pd.Timestamp(self.start_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("start_date debe ser una fecha válida parseable por pandas.") from exc
        if not self.shifts:
            raise ValueError("shifts debe incluir al menos un turno.")
        invalid_shifts = set(self.shifts) - VALID_SHIFTS
        if invalid_shifts:
            raise ValueError(f"shifts contiene turnos inválidos: {sorted(invalid_shifts)}.")
