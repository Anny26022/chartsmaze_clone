import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from filter_mainboard_universe import filter_mainboard_universe, filter_rows_by_symbol


class MainboardUniverseTests(unittest.TestCase):
    def test_excludes_current_nse_sme_symbols_only(self):
        master = [
            {"Symbol": "MAIN", "ISIN": "INE000A01001"},
            {"Symbol": "smeone", "ISIN": "INE000A01002"},
            # A former SME that is absent from the current NSE SME feed is a
            # mainboard listing and must remain eligible.
            {"Symbol": "UPGRADED", "ISIN": "INE000A01003"},
        ]
        sme = [{"Symbol": "SMEONE", "Series": "SM"}]

        mainboard, symbols = filter_mainboard_universe(master, sme)

        self.assertEqual(symbols, {"SMEONE"})
        self.assertEqual([row["Symbol"] for row in mainboard], ["MAIN", "UPGRADED"])

    def test_filters_raw_scanx_rows_to_the_canonical_mainboard_set(self):
        rows = [{"Sym": "MAIN", "Mcap": 10}, {"Sym": "smeone", "Mcap": 1}]
        result = filter_rows_by_symbol(rows, {"MAIN"}, "Sym")
        self.assertEqual(result, [{"Sym": "MAIN", "Mcap": 10}])


if __name__ == "__main__":
    unittest.main()
