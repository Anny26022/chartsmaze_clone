"""Compare local scanner output to dated, user-supplied reference symbol lists.

This intentionally does not contact a third-party screener.  A reference file
is an audit input, not a data source: it keeps validation reproducible and
avoids silently treating a private evaluator as part of this pipeline.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.presets import get_preset
from edl_pipeline.scanner.reference import compare_symbol_sets
from edl_pipeline.scanner.trend import evaluate_universe
from screen_trend_conditions import _load_context


def _read_delivery_history(root: Path) -> dict[str, list[dict]]:
    history: dict[str, list[dict]] = {}
    for path in sorted((root / "delivery_history_data").glob("????-??-??.json")):
        try:
            records = json.loads(path.read_text()).get("records", [])
        except (OSError, ValueError):
            continue
        for record in records:
            if isinstance(record, dict) and record.get("symbol"):
                history.setdefault(str(record["symbol"]).upper(), []).append(record)
    return history


def audit_reference(root: Path, reference: dict) -> dict:
    as_of_date = str(reference["as_of_date"])
    screens = reference.get("screens")
    if not isinstance(screens, dict) or not screens:
        raise ValueError("reference file needs a non-empty screens object")

    context = _load_context(root, as_of_date)
    delivery_history = _read_delivery_history(root)
    output = {}
    for preset_id, reference_symbols in screens.items():
        preset = get_preset(preset_id)
        if preset is None:
            raise ValueError(f"unknown preset: {preset_id}")
        if not isinstance(reference_symbols, list):
            raise ValueError(f"{preset_id}: symbols must be an array")
        evaluated = evaluate_universe(
            root / "ohlcv_data", preset["expression"], as_of_date,
            delivery_history=delivery_history, context_by_symbol=context,
        )
        output[preset_id] = {
            "name": preset["name"],
            "local_counts": evaluated["counts"],
            **compare_symbol_sets(reference_symbols, [item["symbol"] for item in evaluated["results"]]),
        }
    return {
        "schema_version": 1,
        "as_of_date": as_of_date,
        "screens": output,
        "all_exact": all(item["exact"] for item in output.values()),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, type=Path, help="JSON reference file with as_of_date and screens")
    parser.add_argument("--output", type=Path, help="Optional JSON report path; otherwise prints to stdout")
    args = parser.parse_args(argv)
    try:
        reference = json.loads(args.reference.read_text())
        report = audit_reference(ROOT, reference)
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
