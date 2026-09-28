import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import build_filing_history_artifact
import fetch_company_filings
from pipeline_utils import load_json, save_json


class FilingHistoryTests(unittest.TestCase):
    def test_first_fetch_backfills_every_lodr_page_then_deduplicates(self):
        pages = iter([
            ([{"news_id": "legacy", "news_date": "2026-09-01"}], 1, None),
            ([{"news_id": "one", "news_date": "2026-09-03"}], 3, None),
            ([{"news_id": "two", "news_date": "2026-08-03"}], 3, None),
            ([{"news_id": "three", "news_date": "2026-07-03"}], 3, None),
        ])
        with mock.patch.object(fetch_company_filings, "fetch_page", side_effect=lambda *args: next(pages)):
            result = fetch_company_filings.fetch_filings({"Symbol": "ABC", "ISIN": "INE000000001"})
        self.assertEqual(result["status"], "success")
        self.assertTrue(result["history"]["lodr_backfill_complete"])
        self.assertEqual(result["history"]["lodr_total_pages"], 3)
        self.assertEqual([item["news_id"] for item in result["history"]["filings"]], ["one", "legacy", "two", "three"])

    def test_completed_history_refreshes_only_page_one(self):
        existing = {"lodr_backfill_complete": True, "lodr_total_pages": 3, "filings": []}
        with mock.patch.object(fetch_company_filings, "fetch_page", side_effect=[
            ([], 1, None), ([{"news_id": "new", "news_date": "2026-09-25"}], 4, None),
        ]) as pages:
            result = fetch_company_filings.fetch_filings({"Symbol": "ABC", "ISIN": "INE000000001"}, existing)
        self.assertEqual(pages.call_count, 2)
        self.assertTrue(result["history"]["lodr_backfill_complete"])
        self.assertEqual(result["history"]["lodr_total_pages"], 4)

    def test_failed_backfill_remains_pending_for_the_next_run(self):
        with mock.patch.object(fetch_company_filings, "fetch_page", side_effect=[
            ([], 1, None), ([{"news_id": "one"}], 2, None), (None, None, "timeout"),
        ]):
            result = fetch_company_filings.fetch_filings({"Symbol": "ABC", "ISIN": "INE000000001"})
        self.assertFalse(result["history"]["lodr_backfill_complete"])
        self.assertEqual(result["error"], "timeout")

    def test_publishes_persistent_cache_as_a_flat_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            save_json(root / "filing_history_data" / "filing_history.json", {
                "updated_at": "2026-09-25T10:00:00Z",
                "symbols": {"ABC": {"isin": "INE000000001", "lodr_backfill_complete": True, "filings": [{"news_id": "one"}]}},
            })
            with mock.patch.object(build_filing_history_artifact, "BASE_DIR", str(root)):
                self.assertEqual(build_filing_history_artifact.main(), 0)
            payload = load_json(root / "filing_history.json")
        self.assertEqual(payload["coverage"]["lodr_backfill_complete"], 1)
        self.assertEqual(payload["records"][0]["symbol"], "ABC")

    def test_checkpoint_records_completed_and_pending_symbols(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "filing_history_data" / "filing_history.json"
            path.parent.mkdir()
            with mock.patch.object(fetch_company_filings, "HISTORY_FILE", str(path)):
                fetch_company_filings._save_history({
                    "DONE": {"lodr_backfill_complete": True},
                    "PENDING": {"lodr_backfill_complete": False},
                })
            payload = load_json(path)
        self.assertEqual(payload["coverage"], {"symbols": 2, "lodr_backfill_complete": 1, "lodr_backfill_pending": 1})
