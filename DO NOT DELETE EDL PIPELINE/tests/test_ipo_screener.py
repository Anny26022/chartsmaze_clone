import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from build_ipo_screener_artifact import build_ipo_catalog


class IpoScreenerTests(unittest.TestCase):
    def test_uses_official_eq_listing_dates_and_never_invents_issue_terms(self):
        stocks = [{
            "symbol": "NEW", "name": "New Ltd", "isin": "INE1", "security_id": "101",
            "default_screener_eligible": True, "close": 125, "market_cap_crore": 800,
            "sector": "Industry", "industry": "Manufacturing", "rupee_volume": 2_000_000,
        }]
        listings = [
            {"SYMBOL": " NEW ", " SERIES": " EQ ", " DATE OF LISTING": "25-SEP-2026"},
            {"SYMBOL": "SME", " SERIES": " SM ", " DATE OF LISTING": "25-SEP-2026"},
            {"SYMBOL": "PENDING", " SERIES": " EQ ", " DATE OF LISTING": "24-SEP-2026"},
        ]
        records, pending = build_ipo_catalog(stocks, listings, date(2026, 9, 28))
        self.assertEqual(records[0]["symbol"], "NEW")
        self.assertEqual(records[0]["listing_age_calendar_days"], 3)
        self.assertEqual(records[0]["issue_price"], None)
        self.assertEqual(records[0]["anchor_lock_in_end"], None)
        self.assertEqual(pending, [{"symbol": "PENDING", "listing_date": "2026-09-24", "reason": "pending_canonical_enrichment"}])


if __name__ == "__main__":
    unittest.main()
