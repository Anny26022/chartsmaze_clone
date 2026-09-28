import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reconcile_nse_equity_universe import reconcile


class NseUniverseReconciliationTests(unittest.TestCase):
    def test_only_eq_rows_without_scanx_identity_are_pending_or_alerted(self):
        master = [{"Symbol": "MATCHED", "ISIN": "INE000000001"}]
        nse = [
            {"SYMBOL": "MATCHED", " ISIN NUMBER": "INE000000001", " SERIES": "EQ"},
            {"SYMBOL": "NEWIPO", " ISIN NUMBER": "INE000000002", " SERIES": "EQ", " DATE OF LISTING": "25-SEP-2026"},
            {"SYMBOL": "OVERDUE", " ISIN NUMBER": "INE000000003", " SERIES": "EQ", " DATE OF LISTING": "22-SEP-2026"},
            {"SYMBOL": "RIGHTS-RE", " ISIN NUMBER": "INE000000004", " SERIES": "BE", " DATE OF LISTING": "22-SEP-2026"},
        ]
        pending, excluded = reconcile(master, nse, date(2026, 9, 28))
        self.assertEqual([item["symbol"] for item in pending], ["OVERDUE", "NEWIPO"])
        self.assertEqual(pending[0]["status"], "alert")
        self.assertEqual(pending[1]["status"], "pending_scanx_enrichment")
        self.assertEqual(excluded[0]["symbol"], "RIGHTS-RE")


if __name__ == "__main__":
    unittest.main()
