import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for path in (ROOT, SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from edl_pipeline.scanner.query import compile_query
from edl_pipeline.scanner.trend import evaluate_history


def history(length=300):
    dates = pd.date_range("2025-01-01", periods=length, freq="B")
    close = list(range(100, 100 + length))
    return pd.DataFrame({"Date": dates, "Open": close, "High": [value + 2 for value in close], "Low": [value - 2 for value in close], "Close": close, "Volume": [100_000] * length})


class ScannerQueryTests(unittest.TestCase):
    def test_compiles_and_or_with_the_documented_precedence(self):
        tree = compile_query("Market Cap (in Cr) > 2000 OR Close Price > 50 DMA AND Close Price > 100")
        self.assertEqual(tree["op"], "OR")
        self.assertEqual(tree["children"][1]["op"], "AND")

    def test_compiled_query_uses_the_same_local_evaluator(self):
        frame = history()
        tree = compile_query("Market Cap (in Cr) > 2000 AND Close Price > 50 DMA")
        result = evaluate_history(frame, tree, context={"stock": {"as_of_date": "2026-02-24", "market_cap_crore": 3_000, "close": 399}})
        self.assertEqual(result["status"], "match")

    def test_function_query_compiles_to_existing_conditions(self):
        tree = compile_query("ADX(14) > 25 AND MA Stack(\"50,150,200\", SMA, true)")
        self.assertEqual(tree["children"][0]["kind"], "ADX")
        self.assertEqual(tree["children"][1]["kind"], "MA_STACK")

    def test_unpublished_field_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported query field"):
            compile_query("Dividend Cover ratio > 4")


if __name__ == "__main__":
    unittest.main()
