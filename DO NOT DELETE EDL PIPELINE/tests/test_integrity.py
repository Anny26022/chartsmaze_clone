from datetime import date
import gzip
import json
import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from advanced_metrics_processor import process_symbol_csv
from process_earnings_performance import calculate_earnings_metrics
import fetch_fundamental_data
from pipeline_utils import save_json
from edl_pipeline.artifacts import FILES_TO_COMPRESS, FINAL_ARTIFACT_SPECS, PHASE4_SCRIPTS, OHLCV_DERIVED_SCRIPT
from edl_pipeline.publication import promote, main as publish
from edl_pipeline.quality import inspect_publication
from edl_pipeline.transforms.fundamentals import analyze_stock, calculate_change, get_float
from edl_pipeline.validators import validate_json, validate_gzip_json
from build_corporate_action_ledger import build_ledger


class IntegrityTests(unittest.TestCase):
    def history(self, root, count, flat=False):
        rows = []
        for i, day in enumerate(pd.bdate_range('2025-01-01', periods=count)):
            rows.append(dict(Date=str(day.date()), Open=100+i, High=100+i if flat else 102+i,
                             Low=100+i if flat else 99+i, Close=100+i, Volume=1000+i))
        path = root / 'IPO.csv'
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def test_independent_indicator_warmups(self):
        with tempfile.TemporaryDirectory() as tmp:
            for count in (1, 5, 13, 14, 19, 20, 21, 50, 51, 126, 127, 199, 200, 201, 252, 253):
                with self.subTest(count=count):
                    _, metrics = process_symbol_csv(self.history(Path(tmp), count))
                    self.assertIsNotNone(metrics)
                    self.assertEqual(metrics['atr14'] is not None, count >= 14)
                    self.assertEqual(metrics['relative_volume_20'] is not None, count >= 21)
                    self.assertEqual(metrics['close_above_sma200'] is not None, count >= 200)
                    self.assertEqual(metrics['sma50_crossed_above_sma200_today'] is not None, count >= 201)
                    self.assertEqual(metrics['breakout_above_52w_high'] is not None, count >= 253)
                    self.assertEqual(metrics['6 Month Returns(%)'] is not None, count >= 127)

    def test_flat_range_does_not_discard_metrics(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, metrics = process_symbol_csv(self.history(Path(tmp), 21, flat=True))
        self.assertIsNone(metrics['close_near_day_high'])
        self.assertIsNotNone(metrics['relative_volume_20'])

    def test_zero_earnings_base_is_unavailable_not_infinity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'zero.csv'
            path.write_text('Date,Close,High\n2026-01-01,0,0\n2026-01-02,10,11\n')
            self.assertEqual(calculate_earnings_metrics(path, '2026-01-02'), (None, None))
        self.assertEqual(calculate_earnings_metrics('missing', None), (None, None))

    def test_earnings_history_is_sorted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'unsorted.csv'
            path.write_text('Date,Close,High\n2026-01-02,110,120\n2026-01-01,100,101\n')
            self.assertEqual(calculate_earnings_metrics(path, '2026-01-02'), (10.0, 20.0))

    def test_missing_financials_remain_null_but_real_zero_survives(self):
        row = analyze_stock({'Symbol': 'IPO'}, {}, {}, {})
        for key in ('Net Profit Latest Quarter', 'QoQ % Net Profit Latest', 'P/E', 'D/E', 'Forward P/E', 'FII % change QoQ', 'close'):
            self.assertIsNone(row[key], key)
        self.assertEqual(get_float('0'), 0)
        self.assertIsNone(get_float('Infinity'))
        self.assertEqual(calculate_change(0, 10), -100)
        self.assertIsNone(calculate_change(10, 0))

    def test_failed_fundamental_batch_does_not_publish_partial_data(self):
        master=[{'ISIN':'ONE','Symbol':'A'},{'ISIN':'TWO','Symbol':'B'}]
        with mock.patch.object(fetch_fundamental_data,'load_json',return_value=master), \
             mock.patch.object(fetch_fundamental_data,'BATCH_SIZE',1), \
             mock.patch.object(fetch_fundamental_data,'post_json',side_effect=[{'status':'success','data':[{'isin':'ONE'}]},OSError('offline')]), \
             mock.patch.object(fetch_fundamental_data.time,'sleep'), \
             mock.patch.object(fetch_fundamental_data,'save_json') as saved:
            self.assertFalse(fetch_fundamental_data.fetch_fundamental_data())
            saved.assert_not_called()

    def test_nested_nonfinite_serialization_is_strict(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'finite.json'
            save_json(path, {'rows': [{'x': float('inf'), 'y': float('nan'), 'z': 0}]})
            self.assertEqual(json.loads(path.read_text()), {'rows': [{'x': None, 'y': None, 'z': 0}]})

    def test_validator_checks_later_rows_and_overflow_numbers(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'rows.json'
            path.write_text('[{"symbol":"A"},{}]')
            self.assertFalse(validate_json(path, required_fields=('symbol',)).ok)
            for value in ('Infinity', 'NaN', '1e999'):
                path.write_text('[{"value":'+value+'}]')
                self.assertFalse(validate_json(path).ok)
                compressed = Path(tmp) / 'rows.json.gz'
                compressed.write_bytes(gzip.compress(path.read_bytes()))
                self.assertFalse(validate_gzip_json(compressed).ok)

    def test_v2_registered_for_generation_compression_and_validation(self):
        self.assertIn(OHLCV_DERIVED_SCRIPT, PHASE4_SCRIPTS)
        paths = {spec.path for spec in FINAL_ARTIFACT_SPECS}
        for name in ('market_breadth_v2', 'all_indices_history_v2', 'breadth_universe_snapshot'):
            self.assertEqual(FILES_TO_COMPRESS[name+'.json'], name+'.json.gz')
            self.assertIn(name+'.json.gz', paths)

    def fixture(self, root):
        stamp='2026-09-24T12:00:00+00:00'
        bar={'date':'2026-09-24','open':100,'high':102,'low':99,'close':101,'volume':10}
        files={
            'all_stocks_fundamental_analysis.json.gz':[dict(bar, symbol='ABC', as_of_date='2026-09-24', atr14=None)],
            'master_isin_map.json':[{'Symbol':'ABC'}],
            'all_indices_list.json':[{'Symbol':'NIFTY'}],
            'sector_analytics.json.gz':{'sectors':[], 'industries':[]},
            'all_indices_history_v2.json.gz':{'generated_at':stamp,'indices':[{'symbol':'NIFTY','records':[bar]}]},
            'market_breadth_v2.json.gz':{'generated_at':stamp,'records':[{'date':'2026-09-24'}]},
            'breadth_universe_snapshot.json.gz':{'generated_at':stamp},
            'corporate_action_ledger.json.gz':{'source':'test','price_adjusted':False,'records':[]},
            'nse_fno_ban.json.gz':{'source':'test','available':False,'trade_date':None,'symbols':[]},
            'rs_rating_daily.json.gz':{'source':'test','as_of_date':'2026-09-24','ratings':{}},
        }
        for name, data in files.items():
            self.write(root, name, data)
        (root/'market_breadth.json.gz').write_bytes(gzip.compress(b'Type of Info,2026-09-24\nAdvances,1\n'))
        return files

    def write(self, root, name, data):
        raw=json.dumps(data).encode()
        (root/name).write_bytes(gzip.compress(raw) if name.endswith('.gz') else raw)

    def test_whole_publication_and_symbol_availability(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            report=inspect_publication(root, today=date(2026,9,24))
            self.assertEqual(report['errors'], [])
            self.assertIn('atr14', report['symbols'][0]['missing_fields'])
            self.assertEqual(report['coverage']['listing_date']['missing'], 1)
            self.assertFalse(report['corporate_action_ledger']['price_adjusted'])

    def test_stale_mixed_invalid_and_incomplete_publications_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for scenario in ('old_generation','missing_symbol','invalid_ohlc','stale_stock','duplicate_index_date','wrong_breadth_date'):
                with self.subTest(scenario=scenario):
                    data=self.fixture(root)
                    if scenario=='old_generation':data['market_breadth_v2.json.gz']['generated_at']='2026-08-07T12:00:00+00:00'
                    if scenario=='missing_symbol':data['master_isin_map.json'].append({'Symbol':'MISSING'})
                    if scenario=='invalid_ohlc':data['all_stocks_fundamental_analysis.json.gz'][0]['high']=0
                    if scenario=='stale_stock':data['all_stocks_fundamental_analysis.json.gz'][0]['as_of_date']='2026-09-23'
                    if scenario=='duplicate_index_date':data['all_indices_history_v2.json.gz']['indices'][0]['records']*=2
                    if scenario=='wrong_breadth_date':data['market_breadth_v2.json.gz']['records'][0]['date']='2026-09-23'
                    for name,value in data.items():self.write(root,name,value)
                    self.assertTrue(inspect_publication(root,today=date(2026,9,24))['errors'])

    def test_exact_session_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            self.assertTrue(inspect_publication(root,today=date(2026,9,24),expected_session='2026-09-23')['errors'])

    def test_failed_promotion_restores_all_previous_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);stage=root/'stage';stage.mkdir();dest=root/'dest';dest.mkdir()
            for name in ('a','b'):
                (stage/name).write_text('new');(dest/name).write_text('old')
            from pipeline_utils import atomic_replace_bytes
            calls=0
            def failing(path, data):
                nonlocal calls
                calls+=1
                if calls==2:raise OSError('disk failure')
                atomic_replace_bytes(path,data)
            with mock.patch('edl_pipeline.publication.atomic_replace_bytes',side_effect=failing):
                with self.assertRaises(OSError):promote(stage,dest,['a','b'])
            self.assertEqual([(dest/name).read_text() for name in ('a','b')], ['old','old'])

    def test_missing_candidate_never_replaces_any_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);stage=root/'stage';stage.mkdir();dest=root/'dest';dest.mkdir()
            (stage/'a').write_text('new');(dest/'a').write_text('old')
            with self.assertRaises(FileNotFoundError):promote(stage,dest,['a','missing'])
            self.assertEqual((dest/'a').read_text(),'old')

    def test_failed_worker_keeps_published_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'all_indices_list.json').write_text('old')
            with mock.patch('edl_pipeline.publication.pipeline_utils.BASE_DIR',str(root)), mock.patch('edl_pipeline.publication.subprocess.run',return_value=mock.Mock(returncode=1)):
                self.assertEqual(publish(),1)
            self.assertEqual((root/'all_indices_list.json').read_text(),'old')
            self.assertFalse(json.loads((root/'pipeline_failure_report.json').read_text())['published'])

    def test_quality_rejection_keeps_published_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'all_indices_list.json').write_text('old')
            with mock.patch('edl_pipeline.publication.pipeline_utils.BASE_DIR',str(root)), \
                 mock.patch('edl_pipeline.publication.subprocess.run',return_value=mock.Mock(returncode=0)), \
                 mock.patch('edl_pipeline.publication.inspect_publication',return_value={'errors':['stale benchmark']}):
                self.assertEqual(publish(),1)
            self.assertEqual((root/'all_indices_list.json').read_text(),'old')

    def test_success_promotes_artifacts_and_quality_together(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            def worker(command, cwd, env):
                stage=Path(cwd)
                for spec in FINAL_ARTIFACT_SPECS:
                    (stage/spec.path).write_bytes(b'new validated bytes')
                self.write(stage,'pipeline_report.json',{'exit_code':0})
                return mock.Mock(returncode=0)
            with mock.patch('edl_pipeline.publication.pipeline_utils.BASE_DIR',str(root)), \
                 mock.patch('edl_pipeline.publication.subprocess.run',side_effect=worker), \
                 mock.patch('edl_pipeline.publication.inspect_publication',return_value={'errors':[]}):
                self.assertEqual(publish(),0)
            self.assertTrue(json.loads((root/'pipeline_report.json').read_text())['published'])
            self.assertTrue(all((root/spec.path).read_bytes()==b'new validated bytes' for spec in FINAL_ARTIFACT_SPECS))
            self.assertEqual(json.loads((root/'data_quality.json').read_text()),{'errors':[]})

    def test_real_transform_scripts_generate_publishable_outputs_offline(self):
        today=datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            stocks_dir=root/'ohlcv_data';stocks_dir.mkdir()
            indices_dir=root/'indices_ohlcv_data';indices_dir.mkdir()
            # Use dates ending on today's calendar date to isolate publication
            # mechanics from exchange-calendar policy (tested separately).
            dates=pd.date_range(end=today, periods=300, freq='D')
            bars=[{'Date':str(day.date()),'Open':100+i,'High':102+i,'Low':99+i,'Close':101+i,'Volume':1000+i}
                  for i,day in enumerate(dates)]
            frame=pd.DataFrame(bars)
            frame.to_csv(stocks_dir/'ABC.csv',index=False)
            frame.to_csv(indices_dir/'NIFTY.csv',index=False)
            last=bars[-1]
            master=[{'Symbol':'ABC','Name':'ABC Ltd','ISIN':'INE000000001','Sid':1}]
            self.write(root,'master_isin_map.json',master)
            # Empty fundamental body still retains the stock from the master.
            self.write(root,'fundamental_data.json',[])
            self.write(root,'dhan_data_response.json',[{
                'Sym':'ABC','DispSym':'ABC Ltd','Isin':'INE000000001','Sid':1,
                'Mcap':1000,'Ltp':last['Close'],'Open':last['Open'],
                'High':last['High'],'Low':last['Low'],'Volume':last['Volume'],
            }])
            self.write(root,'history_corporate_actions.json',[])
            self.write(root,'all_indices_list.json',[{'Symbol':'NIFTY','IndexID':13,'IndexName':'Nifty 50'}])
            self.write(root,'nse_fno_ban.json',{'source':'test','available':False,'trade_date':None,'symbols':[]})
            shutil.copy2(ROOT/'breadth_methodology.json',root/'breadth_methodology.json')
            env=dict(os.environ,EDL_BASE_DIR=str(root))
            for name in ('bulk_market_analyzer.py','advanced_metrics_processor.py',
                         'process_earnings_performance.py','process_market_breadth.py',
                         'process_historical_market_breadth.py','add_corporate_events.py',
                         'process_mbi_market_breadth.py','build_rs_ratings.py','build_corporate_action_ledger.py','standardize_stock_artifact.py'):
                result=subprocess.run([sys.executable,str(ROOT/name)],cwd=root,env=env,capture_output=True,text=True,timeout=30)
                self.assertEqual(result.returncode,0,name+'\n'+result.stdout+'\n'+result.stderr)
            for raw,compressed in FILES_TO_COMPRESS.items():
                (root/compressed).write_bytes(gzip.compress((root/raw).read_bytes()))
            report=inspect_publication(root,today=today,expected_session=str(today))
            self.assertEqual(report['errors'],[])
            self.assertEqual(report['stock_count'],1)
            self.assertIn('net_profit_latest_quarter',report['symbols'][0]['missing_fields'])

    def test_action_ledger_preserves_unverified_price_actions(self):
        ledger = build_ledger([
            {'Symbol':'ABC','Type':'SPLIT','ExDate':'2026-01-01','RecordDate':'2025-12-30','Details':'Face value changed'},
            {'Symbol':'ABC','Type':'DIVIDEND','ExDate':'2026-01-02','Details':'Rs 1'},
        ])
        self.assertEqual(ledger[0]['adjustment_status'], 'requires_verified_ratio')
        self.assertIsNone(ledger[0]['adjustment_factor'])
        self.assertFalse(ledger[1]['affects_price'])


if __name__=='__main__':unittest.main()
