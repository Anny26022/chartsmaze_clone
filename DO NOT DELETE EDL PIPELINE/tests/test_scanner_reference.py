import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.reference import compare_symbol_sets
from edl_pipeline.scanner.presets import list_presets

# ``audit_reference`` imports the normal CLI context loader, so this unit test
# checks the complete-reference contract at the pure report boundary instead.


class ScannerReferenceTests(unittest.TestCase):
    def test_normalizes_symbols_and_reports_both_directions(self):
        result = compare_symbol_sets(["abc", "ABC", " ", "def"], ["ABC", "ghi"])
        self.assertEqual(result["reference_count"], 2)
        self.assertEqual(result["local_count"], 2)
        self.assertEqual(result["shared_count"], 1)
        self.assertEqual(result["reference_only"], ["DEF"])
        self.assertEqual(result["local_only"], ["GHI"])
        self.assertEqual(result["jaccard"], 0.333333)
        self.assertFalse(result["exact"])

    def test_empty_sets_are_an_exact_match(self):
        result = compare_symbol_sets([], [])
        self.assertTrue(result["exact"])
        self.assertEqual(result["jaccard"], 1.0)

    def test_library_has_the_45_preset_ids_required_for_full_parity(self):
        preset_ids = {preset["id"] for preset in list_presets()}
        self.assertEqual(len(preset_ids), 45)
        self.assertIn("lib-persistent-momentum", preset_ids)
