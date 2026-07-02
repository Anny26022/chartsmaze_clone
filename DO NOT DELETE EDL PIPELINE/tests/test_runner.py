import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.config import PipelineConfig
from edl_pipeline.runner import ScriptResult, build_pipeline_report, main, run_script
from edl_pipeline.validators import ArtifactCheck


class RunnerTests(unittest.TestCase):
    def test_main_returns_nonzero_when_foundation_stage_fails(self):
        with mock.patch("edl_pipeline.runner.run_script", return_value=ScriptResult(False, True)):
            with mock.patch("edl_pipeline.runner.write_pipeline_report"):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(main(PipelineConfig(fetch_ohlcv=False, fetch_optional=False, cleanup_intermediate=False)), 1)

    def test_main_respects_ohlcv_and_optional_flags(self):
        calls = []

        def fake_run_script(script, phase_label="", required=False):
            calls.append(script)
            return ScriptResult(True, required)

        with mock.patch("edl_pipeline.runner.run_script", side_effect=fake_run_script):
            with mock.patch("edl_pipeline.runner.download_nse_listing_dates", return_value=True):
                with mock.patch("edl_pipeline.runner.compress_output", return_value=(100, 10)):
                    with mock.patch("edl_pipeline.runner.validate_final_artifacts", return_value=[]):
                        with mock.patch("edl_pipeline.runner.write_pipeline_report"):
                            with contextlib.redirect_stdout(io.StringIO()):
                                code = main(PipelineConfig(fetch_ohlcv=False, fetch_optional=False, cleanup_intermediate=False))

        self.assertEqual(code, 0)
        self.assertNotIn("fetch_all_ohlcv.py", calls)
        self.assertNotIn("fetch_indices_ohlcv.py", calls)
        self.assertNotIn("fetch_etf_data.py", calls)
        self.assertIn("bulk_market_analyzer.py", calls)

    def test_required_output_validation_failure_marks_script_failed(self):
        completed = mock.Mock(returncode=0)
        failed_check = ArtifactCheck("required.json", "json", False, "missing")

        with mock.patch("edl_pipeline.runner.os.path.exists", return_value=True):
            with mock.patch("edl_pipeline.runner.subprocess.run", return_value=completed):
                with mock.patch("edl_pipeline.runner.validate_script_outputs", return_value=[failed_check]):
                    with contextlib.redirect_stdout(io.StringIO()):
                        result = run_script("required.py", required=True)

        self.assertFalse(result.ok)
        self.assertEqual(result.error, "validation")
        self.assertEqual(result.validations, [failed_check])

    def test_optional_output_validation_failure_stays_warning(self):
        completed = mock.Mock(returncode=0)
        failed_check = ArtifactCheck("optional.json", "json", False, "missing")

        with mock.patch("edl_pipeline.runner.os.path.exists", return_value=True):
            with mock.patch("edl_pipeline.runner.subprocess.run", return_value=completed):
                with mock.patch("edl_pipeline.runner.validate_script_outputs", return_value=[failed_check]):
                    with contextlib.redirect_stdout(io.StringIO()):
                        result = run_script("optional.py", required=False)

        self.assertTrue(result.ok)
        self.assertEqual(result.validations, [failed_check])

    def test_pipeline_report_serializes_script_and_final_checks(self):
        check = ArtifactCheck("artifact.json.gz", "gzip_json", True, count=3)
        report = build_pipeline_report(
            {"script.py": ScriptResult(True, True, validations=[check])},
            total_time=1.23456,
            raw_size=100,
            gz_size=20,
            final_checks=[check],
            config=PipelineConfig(fetch_ohlcv=False, fetch_optional=True, cleanup_intermediate=False),
            exit_code=0,
        )

        self.assertEqual(report["total_time_seconds"], 1.235)
        self.assertEqual(report["config"]["fetch_optional"], True)
        self.assertEqual(report["scripts"]["script.py"]["validations"][0]["path"], "artifact.json.gz")
        self.assertEqual(report["final_artifacts"][0]["count"], 3)


if __name__ == "__main__":
    unittest.main()
