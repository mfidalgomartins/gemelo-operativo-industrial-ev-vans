"""Generador sintético industrial para transición EV en planta de vans."""

from .config import SyntheticGenerationConfig
from .generator import generate_synthetic_factory_data

__all__ = ["SyntheticGenerationConfig", "generate_synthetic_factory_data"]
