"""Deterministic, local scanner condition evaluators."""

from .trend import CONDITION_REGISTRY, evaluate_history, evaluate_universe, evaluate_universe_range

__all__ = ("CONDITION_REGISTRY", "evaluate_history", "evaluate_universe", "evaluate_universe_range")
