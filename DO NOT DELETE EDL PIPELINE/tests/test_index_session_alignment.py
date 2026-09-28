import csv
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fetch_indices_ohlcv import has_current_equity_session


class IndexSessionAlignmentTests(unittest.TestCase):
    def test_current_equity_coverage_allows_after_close_index_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sessions = [(f"S{index}", "2026-09-28") for index in range(9)] + [("STALE", "2026-09-25")]
            for symbol, session in sessions:
                with (root / f"{symbol}.csv").open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=["Date"])
                    writer.writeheader()
                    writer.writerow({"Date": session})
            self.assertTrue(has_current_equity_session(root, "2026-09-28"))

    def test_old_equity_cache_does_not_relabel_index_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for symbol in ("A", "B", "C"):
                with (root / f"{symbol}.csv").open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=["Date"])
                    writer.writeheader()
                    writer.writerow({"Date": "2026-09-25"})
            self.assertFalse(has_current_equity_session(root, "2026-09-28"))

    def test_uses_current_master_universe_not_stale_cache_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for symbol, session in (("MAIN", "2026-09-28"), ("OLD", "2026-09-25")):
                with (root / f"{symbol}.csv").open("w", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=["Date"])
                    writer.writeheader()
                    writer.writerow({"Date": session})
            self.assertTrue(has_current_equity_session(root, "2026-09-28", {"MAIN"}))


if __name__ == "__main__":
    unittest.main()
