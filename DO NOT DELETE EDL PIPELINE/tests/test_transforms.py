import contextlib
import io
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from fetch_bulk_block_deals import date_chunks, dedupe_deals
from fetch_company_filings import dedupe_filings
from fetch_corporate_actions import flatten_actions
from fetch_dhan_data import build_master_map
from fetch_fno_expiry import flatten_expiry_data
from fetch_fno_lot_sizes import clean_lot_size_item
from bulk_market_analyzer import calculate_cagr
from nse_archive_utils import clean_records
from ohlcv_utils import merge_rows_by_date, read_ohlcv_csv, rows_from_tick_data, write_ohlcv_csv
from pipeline_utils import chunked, load_json, save_json
from run_full_pipeline import env_bool


class TransformTests(unittest.TestCase):
    def test_build_master_map_filters_missing_ids_and_sorts_symbols(self):
        stocks = [
            {"Sym": "BETA", "Isin": "INB", "DispSym": "Beta Ltd", "Sid": 2, "FnoFlag": 1},
            {"Sym": "ALPHA", "Isin": "INA", "DispSym": "Alpha Ltd", "Sid": 1},
            {"Sym": "NOISIN", "DispSym": "No ISIN"},
        ]

        self.assertEqual(
            build_master_map(stocks),
            [
                {"Symbol": "ALPHA", "ISIN": "INA", "Name": "Alpha Ltd", "Sid": 1, "FnoFlag": 0},
                {"Symbol": "BETA", "ISIN": "INB", "Name": "Beta Ltd", "Sid": 2, "FnoFlag": 1},
            ],
        )

    def test_calculate_cagr_avoids_complex_values_for_negative_inputs(self):
        self.assertEqual(calculate_cagr(-10, 100, 5), 0.0)
        self.assertEqual(calculate_cagr(100, -10, 5), 0.0)
        self.assertAlmostEqual(calculate_cagr(200, 100, 5), 14.8698, places=3)

    def test_dedupe_filings_prefers_record_with_file_url(self):
        filings = [
            {"news_id": "1", "news_date": "2026-01-01", "caption": "Result"},
            {"news_id": "1", "news_date": "2026-01-01", "caption": "Result", "file_url": "https://example.com/a.pdf"},
            {"news_date": "2026-01-02", "caption": "Board Meeting"},
        ]

        result = dedupe_filings(filings)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[1]["file_url"], "https://example.com/a.pdf")

    def test_dedupe_deals_uses_existing_composite_key(self):
        deal = {"sym": "ABC", "date": "2026-01-01", "qty": 100, "avgprice": 12.3, "bs": "B", "cname": "Buyer"}
        duplicate = dict(deal)
        later = {"sym": "XYZ", "date": "2026-01-02", "qty": 50, "avgprice": 9, "bs": "S", "cname": "Seller"}

        self.assertEqual([d["sym"] for d in dedupe_deals([deal, duplicate, later])], ["XYZ", "ABC"])

    def test_flatten_actions_filters_to_date_window(self):
        raw = [{
            "Sym": "ABC",
            "DispSym": "ABC Ltd",
            "CorpAct": [
                {"ExDate": "2026-01-02", "ActType": "DIVIDEND", "RecDate": "2026-01-03", "Note": "Rs 1"},
                {"ExDate": "2026-02-01", "ActType": "SPLIT"},
            ],
        }]

        self.assertEqual(
            flatten_actions(raw, "2026-01-01", "2026-01-31"),
            [{
                "Symbol": "ABC",
                "Name": "ABC Ltd",
                "Type": "DIVIDEND",
                "ExDate": "2026-01-02",
                "RecordDate": "2026-01-03",
                "Details": "Rs 1",
            }],
        )

    def test_fno_lot_and_expiry_flatteners_preserve_output_schema(self):
        lot_item = {"sym": "ABC", "disp": "ABC Ltd", "fo_dt": [{"sym": "ABC-JUL", "ls": 75}]}
        expiry_raw = [{
            "exch": "NSE",
            "seg": "D",
            "exps": [{"inst": "FUT", "explst": [{"symbolName": "ABC", "expdate": "2026-07-30", "underlyingSecID": 123}]}],
        }]

        self.assertEqual(clean_lot_size_item(lot_item), {"Symbol": "ABC", "Name": "ABC Ltd", "Lot_JUL": 75})
        self.assertEqual(
            flatten_expiry_data(expiry_raw),
            [{
                "Exchange": "NSE",
                "Segment": "D",
                "InstrumentType": "FUT",
                "SymbolName": "ABC",
                "ExpiryDate": "2026-07-30",
                "UnderlyingSecID": 123,
            }],
        )

    def test_ohlcv_rows_merge_and_csv_round_trip(self):
        rows = rows_from_tick_data({
            "Time": ["2026-01-01", "2026-01-02"],
            "o": [1, 2],
            "h": [2, 3],
            "l": [0.5, 1.5],
            "c": [1.5, 2.5],
            "v": [100, 200],
        })

        self.assertEqual(rows[0]["Close"], 1.5)
        self.assertEqual(merge_rows_by_date([rows[1], rows[0], {**rows[1], "Close": 9}])[1]["Close"], 9)

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "ABC.csv"
            write_ohlcv_csv(csv_path, rows)
            self.assertEqual(read_ohlcv_csv(csv_path)[0]["Date"], "2026-01-01")

    def test_shared_json_and_chunk_helpers(self):
        self.assertEqual(list(chunked([1, 2, 3], 2)), [(0, [1, 2]), (2, [3])])

        with tempfile.TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "sample.json"
            save_json(json_path, {"a": 1})
            self.assertEqual(load_json(json_path), {"a": 1})

    def test_clean_records_strips_csv_keys_and_values(self):
        pandas = __import__("pandas")
        df = pandas.DataFrame([{" Symbol ": " ABC ", "Band": " 20 "}])
        self.assertEqual(clean_records(df), [{"Symbol": "ABC", "Band": "20"}])

    def test_env_bool_preserves_defaults_and_parses_common_values(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertTrue(env_bool("MISSING", True))
            self.assertFalse(env_bool("MISSING", False))

        truthy = ["1", "true", "YES", "on"]
        falsy = ["0", "false", "NO", "off"]
        for value in truthy:
            with mock.patch.dict("os.environ", {"FLAG": value}, clear=True):
                self.assertTrue(env_bool("FLAG", False))
        for value in falsy:
            with mock.patch.dict("os.environ", {"FLAG": value}, clear=True):
                self.assertFalse(env_bool("FLAG", True))

        with mock.patch.dict("os.environ", {"FLAG": "not-a-bool"}, clear=True):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertTrue(env_bool("FLAG", True))


if __name__ == "__main__":
    unittest.main()
