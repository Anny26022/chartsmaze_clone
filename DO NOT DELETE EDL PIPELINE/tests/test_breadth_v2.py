import math
import json
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.breadth.aggregates import BreadthAccumulator
from edl_pipeline.breadth.config import BreadthMethodology, load_methodology
from edl_pipeline.breadth.indicators import prepare_history
from edl_pipeline.breadth.indices import generate_all_index_history
from edl_pipeline.breadth.mbi import enrich_records
from edl_pipeline.breadth.pipeline import generate_market_breadth
from edl_pipeline.breadth.universe import build_universe_snapshot


def make_ohlcv(closes, start="2024-01-01"):
    dates = pd.bdate_range(start, periods=len(closes))
    return pd.DataFrame({
        "Date": dates.strftime("%Y-%m-%d"),
        "Open": closes,
        "High": [value + 1 for value in closes],
        "Low": [value - 1 for value in closes],
        "Close": closes,
        "Volume": [1000 + index for index in range(len(closes))],
    })


def blank_aggregate(date):
    row = {
        "date": date,
        "eligible_with_candle": 10,
        "valid_return": 10,
        "advances": 6,
        "declines": 4,
        "unchanged": 0,
        "up_4": 4,
        "down_4": 2,
        "up_4_5": 3,
        "down_4_5": 1,
        "valid_monthly_extrema": 10,
        "new_monthly_high": 3,
        "new_monthly_low": 1,
        "valid_quarterly_extrema": 10,
        "new_quarterly_high": 2,
        "new_quarterly_low": 1,
        "valid_yearly_extrema": 10,
        "new_52w_high": 3,
        "new_52w_low": 1,
        "valid_volume_20": 10,
        "volume_above_20": 6,
        "volume_below_or_equal_20": 4,
        "valid_return_21": 10,
        "up_25_month": 1,
        "down_25_month": 0,
        "up_50_month": 0,
        "down_50_month": 0,
        "valid_return_34": 10,
        "up_13_34d": 2,
        "down_13_34d": 1,
        "valid_return_63": 10,
        "up_25_quarter": 2,
        "down_25_quarter": 1,
    }
    for ma_type in ("sma", "ema"):
        for period in (10, 20, 50, 200):
            row[f"valid_{ma_type}_{period}"] = 10
            row[f"above_{ma_type}_{period}"] = 6
            row[f"below_{ma_type}_{period}"] = 4
            row[f"equal_{ma_type}_{period}"] = 0
    return row


class BreadthV2Tests(unittest.TestCase):
    def setUp(self):
        self.methodology = BreadthMethodology()

    def test_methodology_file_uses_correct_strict_market_cap_threshold(self):
        methodology = load_methodology(ROOT / "breadth_methodology.json")

        self.assertEqual(methodology.minimum_market_cap_crore, 100.0)
        self.assertEqual(methodology.market_cap_comparison, "strictly_greater")
        self.assertEqual(methodology.xp_input_mode, "raw_counts")
        self.assertEqual(methodology.xp_output_multiplier, 0.03136)

    def test_universe_requires_price_at_least_one_and_market_cap_above_100(self):
        rows = [
            {"Sym": "PASS", "Isin": "I1", "Sid": 1, "Ltp": 1, "Mcap": 100.01},
            {"Sym": "EQUAL", "Isin": "I2", "Sid": 2, "Ltp": 1, "Mcap": 100},
            {"Sym": "LOWPRICE", "Isin": "I3", "Sid": 3, "Ltp": 0.99, "Mcap": 5000},
            {"Sym": "NOCAP", "Isin": "I4", "Sid": 4, "Ltp": 10, "Mcap": None},
        ]

        snapshot = build_universe_snapshot(rows, self.methodology, "2026-01-01T00:00:00+00:00")

        self.assertEqual([row["symbol"] for row in snapshot["eligible"]], ["PASS"])
        reasons = {row["symbol"]: row["exclusion_reasons"] for row in snapshot["excluded"]}
        self.assertIn("market_cap_not_strictly_greater", reasons["EQUAL"])
        self.assertIn("price_below_minimum", reasons["LOWPRICE"])
        self.assertIn("missing_market_cap", reasons["NOCAP"])

    def test_indicators_use_prior_window_for_strict_new_high(self):
        closes = [100 + index * 0.1 for index in range(254)]
        prepared = prepare_history(make_ohlcv(closes), self.methodology)

        self.assertTrue(pd.isna(prepared.loc[251, "Yearly_Reference_High"]))
        self.assertFalse(prepared.loc[251, "New_Yearly_High"])
        self.assertTrue(prepared.loc[252, "New_Yearly_High"])
        self.assertAlmostEqual(
            prepared.loc[253, "SMA_200"],
            sum(closes[-200:]) / 200,
        )

    def test_negative_four_boundary_is_strict(self):
        closes = [100.0, 104.0, 99.84, 95.74656]
        prepared = prepare_history(make_ohlcv(closes), self.methodology)
        accumulator = BreadthAccumulator(self.methodology)
        accumulator.update(prepared)
        records = accumulator.records()

        self.assertEqual(records[1]["up_4"], 1)
        self.assertEqual(records[2]["down_4"], 0)
        self.assertEqual(records[3]["down_4"], 1)

    def test_metric_denominators_do_not_include_insufficient_history(self):
        long_history = prepare_history(make_ohlcv([100 + index for index in range(220)]), self.methodology)
        short_history = prepare_history(make_ohlcv([100 + index for index in range(30)], start="2024-09-23"), self.methodology)
        accumulator = BreadthAccumulator(self.methodology)
        accumulator.update(long_history)
        accumulator.update(short_history)
        latest = accumulator.records()[-1]

        self.assertEqual(latest["eligible_with_candle"], 2)
        self.assertEqual(latest["valid_sma_20"], 2)
        self.assertEqual(latest["valid_sma_200"], 1)

        enriched = enrich_records([latest], self.methodology)[0]
        expected_pct = 100 * latest["above_sma_200"] / latest["eligible_with_candle"]
        self.assertAlmostEqual(enriched["above_200_pct"], expected_pct)

    def test_mbi_ratios_changes_scoring_and_xp_are_deterministic(self):
        first = blank_aggregate("2026-01-01")
        second = blank_aggregate("2026-01-02")
        second["up_4"] = 6
        second["down_4"] = 1
        second["up_4_5"] = 6
        second["down_4_5"] = 1
        second["above_sma_20"] = 8
        second["below_sma_20"] = 2
        second["above_sma_50"] = 9
        second["below_sma_50"] = 1

        output = enrich_records(
            [first, second],
            self.methodology,
            {"2026-01-01": 100, "2026-01-02": 102},
        )

        self.assertEqual(output[0]["ratio_4"], 200)
        self.assertEqual(output[1]["ratio_4"], 600)
        self.assertEqual(output[1]["change_4"], 200)
        self.assertEqual(output[1]["ratio_4_5"], 600)
        self.assertEqual(output[1]["change_4_5"], 100)
        self.assertAlmostEqual(output[1]["index_change_pct"], 2)
        self.assertEqual(output[1]["mbi_state"], "green")
        self.assertTrue(math.isfinite(output[0]["xp"]))
        self.assertTrue(math.isfinite(output[1]["xp"]))
        self.assertEqual(output[0]["xp"], output[0]["xp_raw"])
        self.assertAlmostEqual(output[0]["up_4_5_pct"], 30.0)
        self.assertAlmostEqual(output[0]["down_4_5_pct"], 10.0)
        self.assertEqual(output[0]["xp_advancer_count"], 3)
        self.assertEqual(output[0]["xp_decliner_count"], 1)
        self.assertEqual(output[0]["xp_smoothed_advances"], 3)
        self.assertIsNone(output[0]["em"])
        self.assertAlmostEqual(output[0]["new_52w_high_pct"], 30.0)
        self.assertAlmostEqual(output[0]["new_52w_low_pct"], 10.0)
        self.assertAlmostEqual(output[0]["xp"], 10.7142261241, places=6)

    def test_xp_uses_raw_counts_not_universe_percentages(self):
        small_universe = blank_aggregate("2026-01-01")
        large_universe = blank_aggregate("2026-01-01")
        large_universe["eligible_with_candle"] = 100

        small = enrich_records([small_universe], self.methodology)[0]
        large = enrich_records([large_universe], self.methodology)[0]

        self.assertNotEqual(small["up_4_5_pct"], large["up_4_5_pct"])
        self.assertEqual(small["xp_advancer_count"], large["xp_advancer_count"])
        self.assertEqual(
            small["xp_smoothed_advances"],
            large["xp_smoothed_advances"],
        )

    def test_xp_output_calibration_does_not_feed_back_into_recurrence(self):
        rows = [
            blank_aggregate("2026-01-01"),
            blank_aggregate("2026-01-02"),
        ]
        baseline = enrich_records(rows, self.methodology)
        calibrated = enrich_records(
            rows,
            replace(self.methodology, xp_output_multiplier=0.03136),
        )

        for raw_row, calibrated_row in zip(baseline, calibrated):
            self.assertAlmostEqual(calibrated_row["xp_raw"], raw_row["xp_raw"])
            self.assertAlmostEqual(
                calibrated_row["xp"],
                raw_row["xp_raw"] * 0.03136,
            )

    def test_zero_denominator_ratios_are_null_while_xp_remains_finite(self):
        row = blank_aggregate("2026-01-01")
        row["down_4"] = 0
        row["above_sma_20"] = row["eligible_with_candle"]
        row["above_sma_50"] = row["eligible_with_candle"]

        output = enrich_records([row], self.methodology)[0]

        self.assertIsNone(output["ratio_4"])
        self.assertIsNone(output["ratio_20"])
        self.assertIsNone(output["ratio_50"])
        self.assertTrue(math.isfinite(output["xp"]))

    def test_xp_treats_insufficient_ma_history_as_not_above(self):
        first = blank_aggregate("2026-01-01")
        first["up_4_5"] = 10
        first["valid_sma_10"] = 0
        first["valid_sma_20"] = 0
        first["above_sma_10"] = 0
        first["above_sma_20"] = 0
        second = blank_aggregate("2026-01-02")
        second["up_4_5"] = 0

        output = enrich_records([first, second], self.methodology)

        self.assertTrue(math.isfinite(output[0]["xp"]))
        self.assertEqual(output[0]["above_10_pct"], 0)
        self.assertEqual(output[0]["above_20_pct"], 0)
        self.assertEqual(output[0]["xp_smoothed_advances"], 10)
        self.assertAlmostEqual(output[1]["xp_smoothed_advances"], 8.38)
        self.assertTrue(math.isfinite(output[1]["xp"]))

    def test_end_to_end_generator_writes_auditable_artifacts(self):
        universe = [
            {"Sym": "AAA", "DispSym": "AAA Ltd", "Isin": "I1", "Sid": 1, "Ltp": 130, "Mcap": 1500},
            {"Sym": "BBB", "DispSym": "BBB Ltd", "Isin": "I2", "Sid": 2, "Ltp": 80, "Mcap": 2000},
            {"Sym": "SMALL", "DispSym": "Small Ltd", "Isin": "I3", "Sid": 3, "Ltp": 50, "Mcap": 100},
        ]
        dates = pd.bdate_range("2024-01-01", periods=270)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ohlcv = root / "ohlcv"
            ohlcv.mkdir()
            make_ohlcv([100 + index * 0.2 for index in range(270)]).to_csv(ohlcv / "AAA.csv", index=False)
            make_ohlcv([200 - index * 0.1 for index in range(270)]).to_csv(ohlcv / "BBB.csv", index=False)
            pd.DataFrame({
                "Date": dates.strftime("%Y-%m-%d"),
                "Close": [1000 + index for index in range(270)],
            }).to_csv(root / "NIFTY.csv", index=False)

            output_path = root / "breadth.json"
            snapshot_path = root / "snapshot.json"
            artifact, snapshot = generate_market_breadth(
                universe,
                ohlcv,
                root / "NIFTY.csv",
                self.methodology,
                output_path,
                snapshot_path,
                generated_at="2026-01-01T00:00:00+00:00",
            )

            self.assertEqual(snapshot["eligible_count"], 2)
            self.assertEqual(artifact["quality"]["processed_symbols"], 2)
            self.assertEqual(artifact["quality"]["record_count"], 250)
            self.assertEqual(artifact["records"][-1]["valid_sma_200"], 2)
            self.assertIn("xp", artifact["records"][-1])
            expected_table_fields = {
                "date",
                "ratio_4_5",
                "xp",
                "em",
                "change_4_5",
                "ratio_20",
                "change_20",
                "ratio_50",
                "change_50",
                "new_52w_high_pct",
                "new_52w_low_pct",
                "up_4_5_pct",
                "down_4_5_pct",
                "above_10_pct",
                "above_20_pct",
                "above_50_pct",
                "above_200_pct",
                "index_change_pct",
            }
            schema_fields = {column["field"] for column in artifact["table_schema"]}
            self.assertEqual(schema_fields, expected_table_fields)
            self.assertTrue(
                expected_table_fields.issubset(artifact["records"][-1])
            )
            self.assertEqual(json.loads(output_path.read_text(encoding="utf-8"))["methodology"]["version"], "mbi-xp-v2.2")
            self.assertEqual(json.loads(snapshot_path.read_text(encoding="utf-8"))["eligible_count"], 2)

    def test_all_index_history_publishes_every_available_index(self):
        indices = [
            {
                "IndexName": "Nifty 50",
                "Symbol": "NIFTY",
                "IndexID": 13,
                "Exchange": "IDX",
                "Segment": "I",
                "Instrument": "IDX",
                "Ltp": 102,
                "PChng": 0.99,
            },
            {
                "IndexName": "Nifty Midcap",
                "Symbol": "NIFTY MIDCAP",
                "IndexID": 14,
                "Exchange": "IDX",
                "Segment": "I",
                "Instrument": "IDX",
                "Ltp": 202,
                "PChng": 0.5,
            },
            {
                "IndexName": "Missing Index",
                "Symbol": "MISSING",
                "IndexID": 15,
            },
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            index_dir = root / "indices"
            index_dir.mkdir()
            make_ohlcv([100, 101, 102]).to_csv(
                index_dir / "NIFTY.csv",
                index=False,
            )
            make_ohlcv([200, 201, 202]).to_csv(
                index_dir / "NIFTY_MIDCAP.csv",
                index=False,
            )
            output_path = root / "all_indices.json"

            artifact = generate_all_index_history(
                indices,
                index_dir,
                output_path,
                output_sessions=2,
                generated_at="2026-01-01T00:00:00+00:00",
            )

            self.assertEqual(artifact["quality"]["available_indices"], 3)
            self.assertEqual(artifact["quality"]["processed_indices"], 2)
            self.assertEqual(
                artifact["quality"]["missing_history_symbols"],
                ["MISSING"],
            )
            by_symbol = {row["symbol"]: row for row in artifact["indices"]}
            self.assertEqual(set(by_symbol), {"NIFTY", "NIFTY MIDCAP"})
            self.assertEqual(len(by_symbol["NIFTY"]["records"]), 2)
            self.assertAlmostEqual(
                by_symbol["NIFTY"]["records"][-1]["change_pct"],
                100 * (102 / 101 - 1),
                places=6,
            )
            saved = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["quality"]["processed_indices"], 2)

    def test_duplicate_index_symbols_use_index_id_for_distinct_histories(self):
        indices = [
            {"IndexName": "First CAPINS", "Symbol": "CAPINS", "IndexID": 99},
            {"IndexName": "Second CAPINS", "Symbol": "CAPINS", "IndexID": 846},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            index_dir = root / "indices"
            index_dir.mkdir()
            make_ohlcv([100, 101]).to_csv(
                index_dir / "CAPINS__99.csv",
                index=False,
            )
            make_ohlcv([200, 202]).to_csv(
                index_dir / "CAPINS__846.csv",
                index=False,
            )

            artifact = generate_all_index_history(
                indices,
                index_dir,
                root / "all_indices.json",
                output_sessions=2,
            )

            self.assertEqual(artifact["quality"]["processed_indices"], 2)
            closes = {
                row["index_id"]: row["records"][-1]["close"]
                for row in artifact["indices"]
            }
            self.assertEqual(closes, {99: 101, 846: 202})


if __name__ == "__main__":
    unittest.main()
