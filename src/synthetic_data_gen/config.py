from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SyntheticGenerationConfig:
    seed: int = 20260328
    start_date: str = "2025-01-01"
    months: int = 12
    shifts: tuple[str, ...] = ("A", "B", "C")
    output_raw_dir: Path = Path("data/raw/ev_factory")
    output_report_dir: Path = Path("outputs/reports")

    def ensure_valid(self) -> None:
        if self.months < 9 or self.months > 15:
            raise ValueError("months debe estar entre 9 y 15 para cumplir requisitos.")
