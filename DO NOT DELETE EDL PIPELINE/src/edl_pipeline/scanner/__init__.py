"""Deterministic, local scanner condition evaluators."""

from .trend import CONDITION_REGISTRY, evaluate_history, evaluate_universe

__all__ = ("CONDITION_REGISTRY", "evaluate_history", "evaluate_universe")
