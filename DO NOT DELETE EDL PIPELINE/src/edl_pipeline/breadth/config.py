"""Configuration and validation for the breadth methodology."""

from dataclasses import asdict, dataclass
import json
from pathlib import Path


@dataclass(frozen=True)
class BreadthMethodology:
    version: str = "mbi-xp-v2.2"
    universe_mode: str = "latest_snapshot"
    minimum_price: float = 1.0
    minimum_market_cap_crore: float = 100.0
    market_cap_comparison: str = "strictly_greater"
    default_ma_type: str = "SMA"
    ma_periods: tuple = (10, 20, 50, 200)
    advance_threshold: float = 4.0
    extreme_advance_threshold: float = 4.5
    negative_threshold_comparison: str = "strictly_less"
    new_extrema_comparison: str = "strict_prior_window"
    ratio_zero_denominator: str = "null"
    nnh_score_mode: str = "single_comparison_cell"
    monthly_sessions: int = 21
    quarterly_sessions: int = 63
    yearly_sessions: int = 252
    output_sessions: int = 250
    xp_input_mode: str = "raw_counts"
    xp_initial: float = 12.0
    xp_positive_epsilon: float = 0.5
    xp_percentage_epsilon: float = 0.01
    xp_output_multiplier: float = 1.0
    rounding_digits: int = 6

    def validate(self):
        if self.universe_mode != "latest_snapshot":
            raise ValueError("Only latest_snapshot universe mode is currently supported.")
        if self.market_cap_comparison != "strictly_greater":
            raise ValueError("Market-cap comparison must remain strictly_greater.")
        if self.default_ma_type not in {"SMA", "EMA"}:
            raise ValueError("default_ma_type must be SMA or EMA.")
        if self.negative_threshold_comparison != "strictly_less":
            raise ValueError("Negative breadth thresholds must use strictly_less.")
        if self.new_extrema_comparison != "strict_prior_window":
            raise ValueError("New extrema must compare strictly against the prior window.")
        if self.ratio_zero_denominator != "null":
            raise ValueError("Zero ratio denominators must remain null.")
        if self.nnh_score_mode != "single_comparison_cell":
            raise ValueError("NNH scoring must use a single comparison cell.")
        if tuple(self.ma_periods) != (10, 20, 50, 200):
            raise ValueError("ma_periods must contain 10, 20, 50 and 200.")
        if self.minimum_price < 0 or self.minimum_market_cap_crore < 0:
            raise ValueError("Universe thresholds cannot be negative.")
        if self.output_sessions < 1:
            raise ValueError("output_sessions must be positive.")
        if self.xp_input_mode != "raw_counts":
            raise ValueError("XP inputs must use the publicly disclosed raw_counts mode.")
        if self.xp_initial <= 0 or self.xp_positive_epsilon <= 0:
            raise ValueError("XP seed and epsilon must be positive.")
        if not 0 < self.xp_percentage_epsilon < 50:
            raise ValueError("XP percentage epsilon must be between 0 and 50.")
        if self.xp_output_multiplier <= 0:
            raise ValueError("XP output multiplier must be positive.")
        return self

    def to_dict(self):
        data = asdict(self)
        data["ma_periods"] = list(self.ma_periods)
        return data


def load_methodology(path):
    """Load a methodology JSON file, applying dataclass defaults."""
    resolved = Path(path)
    with resolved.open("r", encoding="utf-8") as handle:
        raw = json.load(handle)
    if "ma_periods" in raw:
        raw["ma_periods"] = tuple(raw["ma_periods"])
    return BreadthMethodology(**raw).validate()
