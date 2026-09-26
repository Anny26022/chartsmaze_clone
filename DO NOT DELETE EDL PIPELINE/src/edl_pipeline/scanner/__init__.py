"""Deterministic, local scanner condition evaluators."""

from .trend import CONDITION_REGISTRY, evaluate_history, evaluate_universe, evaluate_universe_range
from .presets import get_preset, list_presets, load_preset_library, validate_preset_library

__all__ = (
    "CONDITION_REGISTRY", "evaluate_history", "evaluate_universe", "evaluate_universe_range",
    "get_preset", "list_presets", "load_preset_library", "validate_preset_library",
)
