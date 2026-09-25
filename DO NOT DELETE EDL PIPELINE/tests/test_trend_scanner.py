import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.trend import CONDITION_REGISTRY, evaluate_history, evaluate_universe


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
    def test_registry_exposes_all_eight_trend_conditions(self):
        self.assertEqual(set(CONDITION_REGISTRY), {
            "persistent_momentum", "price_vs_ema", "ema_shakeout_reclaim", "adx",
            "price_vs_sma", "percent_days_above_ma", "ma_stack", "ma_slope",
        })

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
