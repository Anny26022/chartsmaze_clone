import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import build_quarterly_financial_ledger
from pipeline_utils import save_json, load_json


class QuarterlyFinancialLedgerTests(unittest.TestCase):
    def test_joins_result_disclosure_to_matching_quarter_and_preserves_type(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_json(root / "fundamental_data.json", [{
                "Symbol": "ACME", "isin": "INE000A01001",
                "incomeStat_cq": {"YEAR": "202506|202503", "REVENUE": "100|90", "NET_PROFIT": "10|9", "EPS": "1|0.9"},
                "incomeStat_sq": {"YEAR": "202506", "REVENUE": "80", "NET_PROFIT": "8", "EPS": "0.8"},
            }])
            save_json(root / "filing_history_data" / "filing_history.json", {"symbols": {"ACME": {
                "filings": [
                    {"news_date": "2025-07-28 16:00:00", "descriptor": "Financial Results", "caption": "Quarter ended June 30, 2025", "file_url": "same.pdf"},
                    {"news_date": "2025-07-29 16:00:00", "descriptor": "Financial Results", "caption": "Quarter ended June 30, 2025", "file_url": "same.pdf"},
                    {"news_date": "2025-05-01 16:00:00", "descriptor": "Financial Results", "caption": "Quarter ended March 31, 2025", "file_url": "march.pdf"},
                ]
            }}})
            with mock.patch.object(build_quarterly_financial_ledger, "BASE_DIR", str(root)):
                self.assertEqual(build_quarterly_financial_ledger.main(), 0)
            rows = load_json(root / "quarterly_financial_history.json")["records"]
            self.assertEqual(len(rows), 3)
            june = [row for row in rows if row["quarter_end"] == "2025-06-30"]
            self.assertEqual({row["report_type"] for row in june}, {"CONSOLIDATED", "STANDALONE"})
            self.assertTrue(all(row["filing_date"] == "2025-07-28 16:00:00" for row in june))

    def test_non_result_filing_and_unmatched_quarter_are_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_json(root / "fundamental_data.json", [{
                "Symbol": "ACME", "incomeStat_cq": {"YEAR": "202506", "REVENUE": "100"},
            }])
            save_json(root / "filing_history_data" / "filing_history.json", {"symbols": {"ACME": {
                "filings": [{"news_date": "2025-07-01", "descriptor": "Investor presentation", "caption": "Quarter ended June 30, 2025"}]
            }}})
            with mock.patch.object(build_quarterly_financial_ledger, "BASE_DIR", str(root)):
                self.assertEqual(build_quarterly_financial_ledger.main(), 0)


if __name__ == "__main__":
    unittest.main()
