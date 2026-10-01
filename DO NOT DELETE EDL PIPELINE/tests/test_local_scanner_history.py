import csv
from datetime import date
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from sync_local_scanner_history import refresh_sessions, restore
from ohlcv_utils import read_ohlcv_csv


class LocalScannerHistoryTests(unittest.TestCase):
    def test_restore_checks_isin_and_removes_invalid_and_weekend_rows(self):
        import json
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); source=root/"source"; source.mkdir(); (source/"ohlcv_data").mkdir()
            (source/"master_isin_map.json").write_text(json.dumps([{"Symbol":"TEST","ISIN":"RIGHT"}]))
            (source/"ohlcv_data/TEST.csv").write_text("Date,Open,High,Low,Close,Volume\n2026-09-25,10,11,9,10,100\n2026-09-26,10,11,9,10,100\n2026-09-28,0,0,0,10,100\n")
            count,rejected=restore(source,root,[{"symbol":"TEST","isin":"WRONG"}])
            self.assertEqual(count,0); self.assertEqual(rejected,["TEST"])
            count,_=restore(source,root,[{"symbol":"TEST","isin":"RIGHT"}])
            self.assertEqual(count,1)
            self.assertEqual([r["Date"] for r in read_ohlcv_csv(root/"ohlcv_data/TEST.csv")],["2026-09-25"])

    def test_refresh_accepts_unique_unknown_series_and_rejects_ambiguous_series(self):
        text="SYMBOL,SERIES,DATE1,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,TTL_TRD_QNTY\nTEST,E1,30-Sep-2026,10,11,9,10,100\nAMB,EQ,30-Sep-2026,10,11,9,10,100\nAMB,BE,30-Sep-2026,20,21,19,20,100\n"
        response=Mock(status_code=200,content=text.encode())
        with tempfile.TemporaryDirectory() as folder, patch("sync_local_scanner_history.requests.Session") as session:
            session.return_value.get.return_value=response
            root=Path(folder); (root/"ohlcv_data").mkdir()
            report=refresh_sessions(root,[{"symbol":"TEST"},{"symbol":"AMB"}],date(2026,9,30),date(2026,9,30))
            self.assertEqual(report[0]["symbols"],1)
            self.assertTrue((root/"ohlcv_data/TEST.csv").exists())
            self.assertFalse((root/"ohlcv_data/AMB.csv").exists())
