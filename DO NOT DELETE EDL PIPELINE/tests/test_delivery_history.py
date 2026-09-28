import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fetch_nse_delivery_history import backfill_delivery_history, cached_sessions


def rows_for(day):
    return [{"symbol": "RELIANCE", "date": day.isoformat(), "delivery_percent": 60.0}]


class DeliveryHistoryBackfillTests(unittest.TestCase):
    def test_builds_trading_session_cache_without_counting_weekends(self):
        calls = []

        def fetcher(day, _session):
            calls.append(day)
            return rows_for(day) if day.weekday() < 5 else None

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            downloaded, available, failures = backfill_delivery_history(
                root, sessions=2, max_calendar_days=4, today=date(2026, 9, 28), fetcher=fetcher,
            )
            self.assertEqual((downloaded, available, failures), (2, 2, []))
            self.assertEqual([path.stem for path in cached_sessions(root)], ["2026-09-28", "2026-09-25"])
            self.assertEqual(calls, [date(2026, 9, 28), date(2026, 9, 27), date(2026, 9, 26), date(2026, 9, 25)])

    def test_remembers_old_non_published_days_but_rechecks_recent_ones(self):
        calls = []

        def fetcher(day, _session):
            calls.append(day)
            return None

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "_availability.json").write_text(json.dumps({"not_published": ["2026-09-15"]}))
            _, available, failures = backfill_delivery_history(
                root, sessions=1, max_calendar_days=12, today=date(2026, 9, 26), fetcher=fetcher,
            )
            self.assertEqual(available, 0)
            self.assertEqual(failures, [])
            self.assertNotIn(date(2026, 9, 15), calls)
            manifest = json.loads((root / "_availability.json").read_text())
            self.assertIn("2026-09-15", manifest["not_published"])

