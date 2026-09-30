import importlib.util
import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SPEC = importlib.util.spec_from_file_location("build_rs_ratings", ROOT / "build_rs_ratings.py")
rs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rs)


class FrontWeightedRsTests(unittest.TestCase):
    def test_uses_benchmark_relative_one_month_and_front_weighted_formulae(self):
        dates = pd.date_range("2025-01-01", periods=260, freq="B")
        benchmark = pd.DataFrame({"Date": dates, "Close": [100.0] * len(dates)})
        stock = benchmark.copy()
        stock.loc[stock.index[-1], "Close"] = 120.0
        benchmark.loc[benchmark.index[-1], "Close"] = 110.0

        scores = rs.relative_strength_scores(stock, benchmark, dates[-1].date())

        # Every stock ratio is 1.20 and every benchmark ratio is 1.10.
        self.assertAlmostEqual(scores["one_month"], 1.20 / 1.10)
        self.assertAlmostEqual(scores["three_month"], 1.20 / 1.10)
        self.assertAlmostEqual(scores["six_month"], 1.20 / 1.10)
        self.assertAlmostEqual(scores["twelve_month"], 1.20 / 1.10)
        self.assertAlmostEqual(scores["front_weighted"], 1.20 / 1.10)

    def test_rejects_stale_or_insufficiently_aligned_history(self):
        dates = pd.date_range("2025-01-01", periods=260, freq="B")
        benchmark = pd.DataFrame({"Date": dates, "Close": [100.0] * len(dates)})
        stale = benchmark.iloc[:-1].copy()
        self.assertIsNone(rs.relative_strength_scores(stale, benchmark, dates[-1].date()))
        self.assertIsNone(rs.relative_strength_scores(benchmark.iloc[:259], benchmark, dates[-1].date()))

    def test_maps_strictly_lower_scores_to_bounded_integer_ratings(self):
        ratings = rs.percentile_ratings({"A": 1.0, "B": 1.0, "C": 2.0, "D": 3.0})
        self.assertEqual(ratings["A"], 1)
        self.assertEqual(ratings["B"], 1)
        # The supplied strict-lower / total-universe equation intentionally
        # reaches 99 only in a sufficiently broad cross-section.
        self.assertEqual(ratings["D"], 74)
        self.assertTrue(all(1 <= rating <= 99 for rating in ratings.values()))


if __name__ == "__main__":
    unittest.main()
