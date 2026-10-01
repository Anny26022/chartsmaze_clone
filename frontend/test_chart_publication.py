import gzip
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from chart_publication import complete_release


class Store:
    base_url = 'https://charts.example.com'
    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail
    def archive(self, previous, month):
        self.calls.append(('archive', month, previous['chartObjectPrefix']))
        if self.fail == 'archive': raise RuntimeError('archive failed')
    def upload_charts(self, root, key):
        self.calls.append(('upload', key))
        if self.fail == 'upload': raise RuntimeError('upload failed')
    def write_release(self, manifest, key):
        self.calls.append(('release', key))
        if self.fail == 'release': raise RuntimeError('release failed')


class PublicationTests(unittest.TestCase):
    def fixture(self, root, session='2026-09-30'):
        charts = root / 'source'; charts.mkdir(exist_ok=True)
        (charts / 'TEST.json.gz').write_bytes(gzip.compress(json.dumps({'symbol': 'TEST', 'asOfDate': session}).encode(), mtime=0))
        (charts / 'index.json').write_text(json.dumps({'symbols': 1, 'asOfDate': session}))
        output = root / 'public'; output.mkdir(exist_ok=True)
        manifest = dict(revision='a'*64, sessionDate=session, datasetUrl='/stocks.json', iposUrl='/ipos.json')
        return charts, output, manifest

    def test_local_cache_is_outside_git_revision_and_release_is_pinned(self):
        with tempfile.TemporaryDirectory() as folder:
            charts, output, manifest = self.fixture(Path(folder))
            release = complete_release(charts, output, manifest)
            self.assertFalse((output / 'revisions' / manifest['revision'] / 'charts').exists())
            self.assertTrue((output / 'charts' / release['chartRevision'] / 'TEST.json.gz').exists())
            self.assertEqual(json.loads((output / 'revisions' / manifest['revision'] / 'release.json').read_text()), release)

    def test_rollover_archives_last_successful_trading_session_before_upload(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            charts, output, manifest = self.fixture(root, '2026-07-31')
            previous = complete_release(charts, output, manifest, Store())
            charts, output, manifest = self.fixture(root, '2026-08-03')
            store = Store(); complete_release(charts, output, dict(manifest, revision='b'*64), store)
            self.assertEqual(store.calls[0], ('archive', '2026-07', previous['chartObjectPrefix']))
            self.assertTrue(store.calls[1][1].startswith('daily/2026-08-03/'))

    def test_every_remote_failure_preserves_current_pointer(self):
        for failure in ('archive', 'upload', 'release'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                charts, output, manifest = self.fixture(root)
                complete_release(charts, output, manifest, Store())
                original = (output / 'current.json').read_bytes()
                charts, output, manifest = self.fixture(root, '2026-10-01')
                with self.assertRaises(RuntimeError):
                    complete_release(charts, output, dict(manifest, revision='b'*64), Store(failure))
                self.assertEqual((output / 'current.json').read_bytes(), original)

    def test_same_month_does_not_archive_and_bad_count_rejects_publication(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); charts, output, manifest=self.fixture(root, '2026-09-29')
            complete_release(charts, output, manifest, Store())
            charts, output, manifest=self.fixture(root)
            store=Store(); complete_release(charts, output, dict(manifest,revision='b'*64), store)
            self.assertFalse(any(call[0]=='archive' for call in store.calls))
            original=(output/'current.json').read_bytes()
            (charts/'index.json').write_text(json.dumps({'symbols':2,'asOfDate':'2026-09-30'}))
            with self.assertRaises(RuntimeError): complete_release(charts,output,manifest,store)
            self.assertEqual((output/'current.json').read_bytes(),original)

    def test_old_session_cannot_replace_latest(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);charts,output,manifest=self.fixture(root)
            complete_release(charts,output,manifest,Store())
            charts,output,manifest=self.fixture(root,'2026-09-29')
            with self.assertRaises(RuntimeError): complete_release(charts,output,manifest,Store())

    def test_real_builder_revision_is_deterministic_and_accepted(self):
        import os
        builder = Path(__file__).resolve().parent.parent / 'DO NOT DELETE EDL PIPELINE' / 'build_chart_artifacts.py'
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            stocks=[{'symbol':'TEST','as_of_date':'2026-09-30'},{'symbol':'OTHER','as_of_date':'2026-09-30'}]
            def build(records):
                (root/'all_stocks_fundamental_analysis.json').write_text(json.dumps(records))
                subprocess.run([sys.executable,str(builder)],env=dict(os.environ,EDL_BASE_DIR=str(root)),check=True,capture_output=True)
                return json.loads((root/'chart_artifacts/index.json').read_text())['revision']
            first=build(stocks)
            second=build(list(reversed(stocks)))
            self.assertEqual(first,second)
            output=root/'public';output.mkdir()
            release=complete_release(root/'chart_artifacts',output,dict(revision='a'*64,sessionDate='2026-09-30'))
            self.assertEqual(release['chartRevision'],first)

    def test_retry_preserves_immutable_release_timestamp(self):
        with tempfile.TemporaryDirectory() as folder:
            charts,output,manifest=self.fixture(Path(folder))
            first=complete_release(charts,output,dict(manifest,publishedAt='2026-10-01T01:00:00Z'),Store())
            second=complete_release(charts,output,dict(manifest,publishedAt='2026-10-01T02:00:00Z'),Store())
            self.assertEqual(first,second)
