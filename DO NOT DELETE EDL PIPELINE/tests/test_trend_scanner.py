import sys
import tempfile
import unittest
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.trend import CONDITION_REGISTRY, evaluate_history, evaluate_universe
from edl_pipeline.scanner.context import KIND_ALIASES
from edl_pipeline.scanner.presets import get_preset, list_presets, load_preset_library, validate_preset_library


def rising_history(length=300):
    dates = pd.date_range("2025-01-01", periods=length, freq="B")
    close = [100 + number for number in range(length)]
    return pd.DataFrame({
        "Date": dates.strftime("%Y-%m-%d"),
        "Open": close,
        "High": [value + 2 for value in close],
        "Low": [value - 2 for value in close],
        "Close": close,
        "Volume": [1_000_000 + number for number in range(length)],
    })


class TrendScannerTests(unittest.TestCase):
    def test_vendored_preset_library_is_complete_and_uses_supported_conditions(self):
        library = load_preset_library()
        self.assertEqual(library["schema_version"], 1)
        self.assertEqual(len(library["presets"]), 45)
        self.assertEqual(len({preset["id"] for preset in library["presets"]}), 45)
        self.assertEqual(get_preset("Horizontal Resistance")["id"], "lib-horizontal-resistance")
        self.assertEqual(list_presets()[0]["id"], "lib-persistent-momentum")
        validate_preset_library(CONDITION_REGISTRY)

    def test_every_vendored_preset_executes_with_its_public_default_parameters(self):
        frame = rising_history(400)
        for preset in load_preset_library()["presets"]:
            with self.subTest(preset=preset["id"]):
                result = evaluate_history(frame, preset["expression"], context={"stock": {"symbol": "TEST"}})
                self.assertIn(result["status"], {"match", "no_match", "unavailable"})

    def test_registry_exposes_trend_momentum_and_volume_conditions(self):
        self.assertEqual(set(CONDITION_REGISTRY), {
            "persistent_momentum", "price_vs_ema", "ema_shakeout_reclaim", "adx",
            "price_vs_sma", "percent_days_above_ma", "ma_stack", "ma_slope",
            "price_change_percent", "consecutive_up_days", "gap_up", "gap_down",
            "relative_volume", "volume_trend", "highest_volume", "delivery_percent_spike",
            "new_high", "new_low", "percent_from_52w_high", "percent_from_52w_low",
            "consolidation_range", "atr_percent", "range_contraction", "inside_bar",
            "unfilled_gap", "vcp_contraction_legs", "horizontal_resistance_line",
            "relative_strength", "rs_new_high", "rs_rating", "market_cap",
            "free_float_market_cap", "pe_ratio", "earnings_growth", "days_since_earnings",
            "sector", "industry", "average_turnover", "adr_percent", "price_range",
            "price_band", "circuit_band_minimum", "series", "listing_age_days",
            "index_membership", "market_breadth", "fno_ban",
        })

    def test_live_bundle_condition_contract_remains_mapped(self):
        fixture = json.loads((ROOT / "tests" / "fixtures" / "journaltoday_screener_contract.json").read_text())
        self.assertEqual(len(fixture["condition_kinds"]), 47)
        self.assertEqual({KIND_ALIASES[kind] for kind in fixture["condition_kinds"]}, set(CONDITION_REGISTRY))

    def test_nested_expression_uses_three_valued_boolean_logic(self):
        frame = rising_history(80)
        expression = {
            "type": "group", "op": "OR", "children": [
                {"type": "condition", "kind": "PRICE_CHANGE_PCT", "params": {"overDays": 5, "comparison": "ABOVE", "pct": 1}},
                {"type": "condition", "kind": "ADX", "params": {"period": 1000, "comparison": "ABOVE", "value": 25}},
            ],
        }
        result = evaluate_history(frame, expression)
        self.assertEqual(result["status"], "match")
        self.assertEqual(result["expression"]["op"], "OR")
        self.assertEqual(result["expression"]["children"][1]["status"], "unavailable")

    def test_range_conditions_use_high_low_history_and_recent_signal_dates(self):
        frame = rising_history()
        high = evaluate_history(frame, [{"condition": "new_high", "lookback_days": 252, "fired_within": 1}])
        low = evaluate_history(frame, [{"condition": "new_low", "lookback_days": 252, "fired_within": 1}])
        distance = evaluate_history(frame, [{"condition": "percent_from_52w_high", "comparison": "less", "value": 2}])
        self.assertEqual(high["status"], "match")
        self.assertEqual(low["status"], "no_match")
        self.assertEqual(distance["status"], "match")
        self.assertEqual(high["conditions"][0]["details"]["days_since_signal"], 0)

    def test_52_week_distance_uses_available_post_listing_history(self):
        frame = rising_history(80)
        high = evaluate_history(frame, [
            {"condition": "percent_from_52w_high", "comparison": "less", "value": 2},
        ])
        low = evaluate_history(frame, [
            {"condition": "percent_from_52w_low", "comparison": "greater", "value": 2},
        ])
        self.assertEqual(high["status"], "match")
        self.assertEqual(low["status"], "match")
        self.assertEqual(high["conditions"][0]["details"]["sessions"], 80)
        self.assertEqual(low["conditions"][0]["details"]["sessions"], 80)

    def test_consolidation_atr_and_prior_range_contraction(self):
        frame = rising_history(90)
        frame.loc[frame.index[-60:-10], "High"] = 140
        frame.loc[frame.index[-60:-10], "Low"] = 80
        frame.loc[frame.index[-20:], ["Open", "High", "Low", "Close"]] = [100, 101, 99, 100]
        result = evaluate_history(frame, [
            {"condition": "consolidation_range", "lookback_days": 10, "max_range_percent": 3, "exclude_latest": 0},
            {"condition": "atr_percent", "period": 3, "comparison": "less", "value": 5},
            {"condition": "range_contraction", "recent_days": 10, "prior_days": 50, "max_ratio": .2, "prior_mode": "prior"},
        ])
        self.assertEqual(result["status"], "match")
        self.assertTrue(all(item["status"] == "match" for item in result["conditions"]))

    def test_daily_and_iso_weekly_inside_bars(self):
        frame = rising_history(20)
        frame.loc[frame.index[-3], ["High", "Low"]] = [150, 50]
        frame.loc[frame.index[-2], ["High", "Low"]] = [140, 60]
        frame.loc[frame.index[-1], ["High", "Low"]] = [130, 70]
        daily = evaluate_history(frame, [{"condition": "inside_bar", "timeframe": "daily", "consecutive": 2}])
        self.assertEqual(daily["status"], "match")

        weekly = rising_history(15)
        weekly.loc[weekly.index[-10:-5], ["High", "Low"]] = [160, 40]
        weekly.loc[weekly.index[-5:], ["High", "Low"]] = [150, 50]
        weekly_result = evaluate_history(weekly, [{"condition": "inside_bar", "timeframe": "weekly", "consecutive": 1}])
        self.assertEqual(weekly_result["status"], "match")

    def test_gap_state_uses_prior_close_as_the_fill_level(self):
        frame = rising_history(30)
        frame[["Open", "High", "Low", "Close"]] = frame[["Open", "High", "Low", "Close"]].astype(float)
        prior_close = frame["Close"].iloc[-3]
        frame.loc[frame.index[-2], ["Open", "High", "Low", "Close"]] = [prior_close * 1.08, prior_close * 1.09, prior_close * 1.06, prior_close * 1.07]
        frame.loc[frame.index[-1], "Low"] = prior_close + 1
        unfilled = evaluate_history(frame, [{"condition": "unfilled_gap", "direction": "up", "minimum_gap_percent": 5, "within_days": 3, "state": "unfilled"}])
        frame.loc[frame.index[-1], "Low"] = prior_close
        filled = evaluate_history(frame, [{"condition": "unfilled_gap", "direction": "up", "minimum_gap_percent": 5, "within_days": 3, "state": "filled"}])
        self.assertEqual(unfilled["status"], "match")
        self.assertEqual(filled["status"], "match")

    def test_vcp_legs_and_horizontal_resistance_are_deterministic(self):
        close = [100] * 250 + [100, 120, 100, 115, 105, 112, 108, 111, 109, 110]
        dates = pd.date_range("2025-01-01", periods=len(close), freq="B")
        frame = pd.DataFrame({
            "Date": dates.strftime("%Y-%m-%d"), "Open": close,
            "High": [value + 1 for value in close], "Low": [value - 1 for value in close],
            "Close": close, "Volume": [100] * len(close),
        })
        vcp = evaluate_history(frame, [{"condition": "vcp_contraction_legs", "minimum_legs": 3, "lookback_days": 20, "max_final_leg_percent": 8, "max_leg_ratio": .9, "minimum_swing_percent": 1.5}])
        resistance = evaluate_history(frame, [{"condition": "horizontal_resistance_line", "lookback_days": 100, "minimum_swing_percent": 1.5, "cluster_tolerance_percent": 3, "minimum_base_length_days": 4, "maximum_base_length_days": 100, "minimum_base_depth_percent": 0, "maximum_base_depth_percent": 60, "maximum_percent_below_line": 20, "maximum_percent_below_20ema": 20}])
        self.assertEqual(vcp["status"], "match")
        self.assertEqual(resistance["status"], "match")

    def test_horizontal_resistance_ignores_zero_price_pivots(self):
        frame = rising_history(300)
        frame["High"] = 0
        frame["Low"] = 0
        result = evaluate_history(frame, [{
            "condition": "horizontal_resistance_line", "lookback_days": 100,
            "minimum_swing_percent": 1.5, "cluster_tolerance_percent": 2.5,
            "minimum_base_length_days": 15, "maximum_percent_below_line": 5,
            "maximum_percent_below_20ema": 2,
        }])
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["conditions"][0]["details"]["reason"], "no_swing_high_pivot")

    def test_momentum_and_volume_conditions_match_a_clear_signal(self):
        frame = rising_history(80)
        frame.loc[frame.index[-1], ["Open", "High", "Low", "Close", "Volume"]] = [182, 184, 179, 180, 10_000_000]
        result = evaluate_history(frame, [
            {"condition": "price_change_percent", "window": 5, "comparison": "greater", "value": 1},
            {"condition": "consecutive_up_days", "minimum_up_days": 3, "fired_within": 1},
            {"condition": "gap_up", "minimum_gap_percent": 1, "fired_within": 1},
            {"condition": "relative_volume", "average_window": 20, "multiple": 2, "fired_within": 1},
            {"condition": "volume_trend", "recent_window": 5, "base_window": 20, "comparison": "greater", "value": 1},
            {"condition": "highest_volume", "lookback": 20, "fired_within": 1, "closed_up": True},
        ])
        self.assertEqual(result["status"], "match")
        self.assertTrue(all(item["status"] == "match" for item in result["conditions"]))

    def test_gap_down_and_fired_within_are_evaluated_on_historical_sessions(self):
        frame = rising_history(30)
        frame.loc[frame.index[-2], "Open"] = int(frame.loc[frame.index[-3], "Close"] * .90)
        result = evaluate_history(frame, [{"condition": "gap_down", "minimum_gap_percent": 5, "fired_within": 2}])
        self.assertEqual(result["status"], "match")
        self.assertEqual(result["conditions"][0]["details"]["days_since_signal"], 1)

    def test_delivery_spike_uses_date_aligned_history_and_never_guesses_missing_days(self):
        frame = rising_history(10)
        records = [{"date": frame["Date"].iloc[-2], "delivery_percent": 62.5}]
        matched = evaluate_history(frame, [{"condition": "delivery_percent_spike", "minimum_delivery_percent": 60, "fired_within": 2}], delivery_history=records)
        missing = evaluate_history(frame, [{"condition": "delivery_percent_spike", "minimum_delivery_percent": 60, "fired_within": 2}])
        self.assertEqual(matched["status"], "match")
        self.assertEqual(matched["conditions"][0]["details"]["days_since_signal"], 1)
        self.assertEqual(missing["status"], "unavailable")

    def test_all_trend_conditions_match_a_clear_rising_series(self):
        frame = rising_history()
        # A genuine low-side dip, followed by a close above EMA, gives shakeout
        # semantics without disturbing the rising close trend.
        frame.loc[frame.index[-2], "Low"] = 1
        request = [
            {"condition": "persistent_momentum", "periods": [10, 20, 50], "persist_days": 10},
            {"condition": "price_vs_ema", "period": 20, "comparison": "above", "persist_days": 10},
            {"condition": "ema_shakeout_reclaim", "period": 20, "dip_within": 3, "dip_basis": "low"},
            {"condition": "adx", "period": 14, "comparison": "greater_or_equal", "value": 25},
            {"condition": "price_vs_sma", "period": 20, "comparison": "above", "persist_days": 10},
            {"condition": "percent_days_above_ma", "period": 20, "ma_type": "sma", "window": 10, "comparison": "equal", "value": 100},
            {"condition": "ma_stack", "periods": [50, 150, 200], "ma_type": "sma", "price_above_fastest": True},
            {"condition": "ma_slope", "period": 20, "ma_type": "ema", "window": 10, "comparison": "greater", "value": 0},
        ]
        result = evaluate_history(frame, request)
        self.assertEqual(result["status"], "match")
        self.assertTrue(all(item["status"] == "match" for item in result["conditions"]))

    def test_strict_persistence_fails_but_reclaim_mode_accepts_one_reclaimed_breach(self):
        frame = rising_history(40)
        frame.loc[frame.index[-3], ["Open", "High", "Low", "Close"]] = [80, 150, 79, 80]
        frame.loc[frame.index[-2], ["Open", "High", "Low", "Close"]] = [140, 155, 139, 140]
        frame.loc[frame.index[-1], ["Open", "High", "Low", "Close"]] = [141, 156, 140, 141]
        strict = evaluate_history(frame, [{
            "condition": "price_vs_ema", "period": 5, "comparison": "above", "persist_days": 3,
            "persistence_mode": "strict_close",
        }])
        reclaim = evaluate_history(frame, [{
            "condition": "price_vs_ema", "period": 5, "comparison": "above", "persist_days": 3,
            "persistence_mode": "reclaim_by_extreme",
        }])
        self.assertEqual(strict["status"], "no_match")
        self.assertEqual(reclaim["status"], "match")

    def test_persistent_momentum_defaults_to_one_reclaimed_breach(self):
        frame = rising_history(40)
        frame.loc[frame.index[-3], ["Open", "High", "Low", "Close"]] = [80, 150, 79, 80]
        frame.loc[frame.index[-2], ["Open", "High", "Low", "Close"]] = [140, 155, 139, 140]
        frame.loc[frame.index[-1], ["Open", "High", "Low", "Close"]] = [141, 156, 140, 141]
        default = evaluate_history(frame, [{
            "condition": "persistent_momentum", "periods": [5], "persist_days": 3,
        }])
        strict = evaluate_history(frame, [{
            "condition": "persistent_momentum", "periods": [5], "persist_days": 3,
            "persistence_mode": "strict_close",
        }])
        self.assertEqual(default["status"], "match")
        self.assertEqual(default["conditions"][0]["details"]["persistence_mode"], "reclaim_by_extreme")
        self.assertEqual(strict["status"], "no_match")

    def test_insufficient_history_is_unavailable_not_a_false_match(self):
        result = evaluate_history(rising_history(20), [{
            "condition": "ma_stack", "periods": [50, 150, 200], "ma_type": "sma",
        }])
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["conditions"][0]["details"]["reason"], "insufficient_history")

    def test_universe_ands_conditions_and_omits_non_matches_by_default(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rising_history(80).to_csv(root / "MATCH.csv", index=False)
            declining = rising_history(80)
            declining["Close"] = list(reversed(declining["Close"].tolist()))
            declining.to_csv(root / "MISS.csv", index=False)
            result = evaluate_universe(root, [{
                "condition": "price_vs_sma", "period": 20, "comparison": "above", "persist_days": 5,
            }])
        self.assertEqual(result["counts"], {"match": 1, "no_match": 1, "unavailable": 0})
        self.assertEqual([row["symbol"] for row in result["results"]], ["MATCH"])
