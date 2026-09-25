import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.history import build_snapshot, load_snapshot


class ScannerHistoryTests(unittest.TestCase):
    def test_persists_only_point_in_time_scanner_fields(self):
        stocks = [{
            "symbol": "RELIANCE", "as_of_date": "2026-09-25", "close": 1400,
            "market_cap_crore": 1, "index_memberships": ["NIFTY 50"], "unrelated": "omit",
        }]
        breadth = {"records": [{"date": "2026-09-25", "above_50_pct": 55.2}]}
        fno = {"available": True, "trade_date": "2026-09-25", "symbols": ["RELIANCE"]}
        with tempfile.TemporaryDirectory() as directory:
            path = build_snapshot(Path(directory), stocks, breadth, fno, "2026-09-25")
            saved = load_snapshot(Path(directory), "2026-09-25")
        self.assertEqual(path.name, "2026-09-25.json.gz")
        self.assertEqual(saved["stocks"], [{"symbol": "RELIANCE", "as_of_date": "2026-09-25", "close": 1400, "market_cap_crore": 1, "free_float_percent": None, "pe_ratio": None, "latest_earnings_date": None, "sector": None, "industry": None, "circuit_limit": None, "listing_date": None, "listing_series": None, "delivery_series": None, "index_memberships": ["NIFTY 50"], "qoq_percent_net_profit_latest": None, "yoy_percent_net_profit_latest": None, "qoq_percent_sales_latest": None, "yoy_percent_sales_latest": None, "qoq_percent_pbt_latest": None, "yoy_percent_pbt_latest": None, "qoq_percent_eps_latest": None, "yoy_percent_eps_latest": None}])
        self.assertEqual(saved["breadth"]["above_50_pct"], 55.2)
        self.assertNotIn("unrelated", saved["stocks"][0])
