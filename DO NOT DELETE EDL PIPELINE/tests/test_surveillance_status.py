import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enrich_surveillance_status import apply_surveillance_status


class SurveillanceStatusTests(unittest.TestCase):
    def test_attaches_stage_and_session_to_matching_stock(self):
        stocks = [
            {"Symbol": "ASMCO", "as_of_date": "2026-09-30"},
            {"Symbol": "GSMCO", "as_of_date": "2026-09-30"},
            {"Symbol": "CLEAR", "as_of_date": "2026-09-30"},
        ]
        available = apply_surveillance_status(
            stocks,
            {"ASMCO": "LTASM - I (13)"},
            {"GSMCO": "GSM Stage 2"},
            "2026-10-01T04:00:00+00:00",
        )
        self.assertTrue(available)
        self.assertTrue(stocks[0]["is_asm"])
        self.assertEqual(stocks[0]["asm_stage"], "LTASM - I (13)")
        self.assertFalse(stocks[0]["is_gsm"])
        self.assertTrue(stocks[1]["is_gsm"])
        self.assertFalse(stocks[2]["is_asm"])
        self.assertEqual(stocks[2]["surveillance_as_of_date"], "2026-09-30")
        self.assertEqual(stocks[2]["surveillance_fetched_at"], "2026-10-01T04:00:00+00:00")

    def test_missing_one_list_marks_every_row_unavailable(self):
        stocks = [{"Symbol": "ASMCO", "as_of_date": "2026-09-30"}]
        available = apply_surveillance_status(stocks, {"ASMCO": "LTASM - I"}, None, "2026-10-01T04:00:00+00:00")
        self.assertFalse(available)
        self.assertIsNone(stocks[0]["is_asm"])
        self.assertIsNone(stocks[0]["is_gsm"])
        self.assertFalse(stocks[0]["surveillance_available"])
        self.assertIsNone(stocks[0]["surveillance_as_of_date"])
