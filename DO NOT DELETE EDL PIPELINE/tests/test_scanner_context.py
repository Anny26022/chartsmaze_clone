import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.context import normalize_condition_spec
from edl_pipeline.scanner.trend import evaluate_history
from nse_fno_ban import parse_report, parse_symbols


def history(length=300):
    dates = pd.date_range("2025-01-01", periods=length, freq="B")
    close = [100 + index for index in range(length)]
    return pd.DataFrame({"Date": dates, "Open": close, "High": [item + 2 for item in close], "Low": [item - 2 for item in close], "Close": close, "Volume": [1_000_000] * length})


class ScannerContextTests(unittest.TestCase):
    def test_journaltoday_payload_is_normalized_for_existing_and_new_rules(self):
        momentum = normalize_condition_spec({"kind": "PRICE_CHANGE_PCT", "params": {"overDays": 5, "comparison": "ABOVE", "pct": 4}})
        trend = normalize_condition_spec({"kind": "PERSISTENT_MOMENTUM", "params": {"ema10Days": 20, "ema20Days": 30, "ema50Days": 50}})
        pattern = normalize_condition_spec({"kind": "RANGE_CONTRACTION", "params": {"recentDays": 10, "priorDays": 60, "maxRatio": .5, "priorMode": "PRIOR"}})
        self.assertEqual(momentum, {"condition": "price_change_percent", "overDays": 5, "comparison": "greater_or_equal", "pct": 4, "window": 5, "value": 4})
        self.assertEqual(trend["persist_days"], {10: 20, 20: 30, 50: 50})
        self.assertEqual(pattern["prior_mode"], "prior")
        breadth = normalize_condition_spec({"kind": "MARKET_BREADTH", "params": {"universe": "ALL_ACTIVE", "metric": "pctAboveSma50", "comparison": "ABOVE", "value": 50}})
        self.assertEqual(breadth["metric"], "pct_above_sma50")
        shakeout = normalize_condition_spec({"kind": "EMA_SHAKEOUT", "params": {"period": 20, "withinDays": 10}})
        rs_high = normalize_condition_spec({"kind": "RS_NEW_HIGH", "params": {"benchmark": "NIFTY_50", "lookbackDays": 60, "minPriceBelowHighPct": 2}})
        self.assertEqual(shakeout["dip_within"], 10)
        self.assertEqual(rs_high["minimum_price_below_high_percent"], 2)

    def test_snapshot_liquidity_and_cross_series_rules(self):
        frame = history()
        benchmark = pd.DataFrame({"date": frame["Date"], "close": [100 + index / 2 for index in range(len(frame))]})
        stock = {"symbol": "TEST", "as_of_date": "2026-02-24", "close": 399, "market_cap_crore": 10_000, "free_float_percent": 40, "pe_ratio": 25, "sector": "IT", "industry": "Software", "listing_series": "EQ", "listing_date": "2024-01-01", "latest_earnings_date": "2026-02-01", "qoq_percent_net_profit_latest": 25, "circuit_limit": "20%", "index_memberships": ["NIFTY 500"]}
        context = {"stock": stock, "benchmarks": {"NIFTY_50": benchmark}, "breadth": {"all_active": {"pct_above_sma50": 60}}, "breadth_as_of": "2026-02-24", "fno_ban_symbols": {"TEST": True}}
        result = evaluate_history(frame, [
            {"kind": "MARKETCAP", "params": {"comparison": "ABOVE", "valueCr": 5_000}},
            {"kind": "FF_MARKETCAP", "params": {"comparison": "ABOVE", "valueCr": 3_000}},
            {"kind": "RELATIVE_STRENGTH", "params": {"benchmark": "NIFTY_50", "overDays": 20, "comparison": "ABOVE", "pct": 1}},
            {"kind": "AVG_TURNOVER", "params": {"comparison": "ABOVE", "windowMinutes": "", "lookbackDays": 20, "valueCr": 1}},
            {"kind": "MARKET_BREADTH", "params": {"universe": "ALL_ACTIVE", "metric": "pct_above_sma50", "comparison": "ABOVE", "value": 50}},
            {"kind": "FNO_BAN", "params": {"mode": "ONLY"}},
        ], context=context)
        self.assertEqual(result["status"], "match")

    def test_point_in_time_rules_do_not_reuse_future_snapshots(self):
        frame = history()
        stock = {"symbol": "TEST", "as_of_date": "2026-02-24", "close": 399, "pe_ratio": 20, "sector": "IT", "latest_earnings_date": "2026-02-01", "qoq_percent_net_profit_latest": 25}
        result = evaluate_history(frame, [
            {"kind": "PE_RATIO", "params": {"comparison": "BELOW", "value": 30}},
            {"kind": "SECTOR", "params": {"values": ["IT"]}},
            {"kind": "EARNINGS_GROWTH", "params": {"metric": "NET_PROFIT", "basis": "QOQ", "comparison": "ABOVE", "pct": 10}},
        ], as_of_date="2025-12-31", context={"stock": stock})
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(
            {item["details"]["reason"] for item in result["conditions"]},
            {"pe_snapshot_not_aligned_to_screen_date", "snapshot_not_aligned_to_screen_date", "earnings_snapshot_not_aligned_to_screen_date"},
        )

    def test_fno_ban_parser_accepts_an_empty_report_and_symbols(self):
        self.assertEqual(parse_report("Securities in Ban For Trade Date 28-SEP-2026:\n"), ("2026-09-28", []))
        self.assertEqual(parse_symbols("Securities in Ban For Trade Date 28-SEP-2026:\n1,KAYNES\n2,LICHSGFIN\n"), ["KAYNES", "LICHSGFIN"])

    def test_stale_rs_rating_and_unavailable_ban_fail_closed(self):
        frame = history()
        base = {"stock": {"symbol": "TEST"}, "rs_ratings": {"TEST": {"three_month": 95}}, "rs_ratings_as_of": "2026-01-01", "fno_ban_available": False}
        result = evaluate_history(frame, [
            {"kind": "RS_RATING", "params": {"window": "three_month", "comparison": "ABOVE", "value": 80}},
            {"kind": "FNO_BAN", "params": {"mode": "EXCLUDE"}},
        ], context=base)
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(
            {item["details"]["reason"] for item in result["conditions"]},
            {"rs_rating_not_aligned_to_screen_date", "fno_ban_snapshot_unavailable"},
        )
