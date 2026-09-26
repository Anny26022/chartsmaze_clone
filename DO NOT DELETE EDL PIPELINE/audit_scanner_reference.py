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

import pandas as pd

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.presets import get_preset, list_presets
from edl_pipeline.scanner.reference import compare_symbol_sets
from edl_pipeline.scanner.trend import evaluate_history
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
    known_presets = {preset["id"] for preset in list_presets()}
    # Input files may use human-facing preset names or stable library IDs.
    # Resolve both to IDs before assessing completeness; otherwise a complete
    # capture exported from the source UI is falsely reported as unknown.
    resolved_screens = {}
    for input_key, reference_symbols in screens.items():
        preset = get_preset(input_key)
        preset_id = preset["id"]
        if preset_id in resolved_screens:
            raise ValueError(f"duplicate preset reference: {preset_id}")
        resolved_screens[preset_id] = (preset, reference_symbols)
    missing_presets = sorted(known_presets - set(resolved_screens))

    # Read each local history once, then evaluate every requested preset from
    # that in-memory frame. A preset-first audit rereads the entire 98 MB
    # universe 45 times, turning one same-session comparison into ~4.4 GB of
    # repeated I/O.
    output = {}
    local_matches = {preset_id: [] for preset_id in resolved_screens}
    local_counts = {
        preset_id: {"match": 0, "no_match": 0, "unavailable": 0}
        for preset_id in resolved_screens
    }
    for preset_id, (_, reference_symbols) in resolved_screens.items():
        if not isinstance(reference_symbols, list):
            raise ValueError(f"{preset_id}: symbols must be an array")
    for path in sorted((root / "ohlcv_data").glob("*.csv")):
        symbol = path.stem
        stock_context = dict(context)
        stock_context["stock"] = (stock_context.get("stocks") or {}).get(symbol, {})
        history = pd.read_csv(path)
        for preset_id, (preset, _) in resolved_screens.items():
            outcome = evaluate_history(
                history, preset["expression"], as_of_date,
                delivery_history.get(symbol, []), stock_context,
            )
            local_counts[preset_id][outcome["status"]] += 1
            if outcome["status"] == "match":
                local_matches[preset_id].append(symbol)
    for preset_id, (preset, reference_symbols) in resolved_screens.items():
        output[preset_id] = {
            "name": preset["name"],
            "local_counts": local_counts[preset_id],
            **compare_symbol_sets(reference_symbols, local_matches[preset_id]),
        }
    return {
        "schema_version": 1,
        "as_of_date": as_of_date,
        "expected_preset_count": len(known_presets),
        "provided_preset_count": len(resolved_screens),
        "missing_presets": missing_presets,
        "complete": not missing_presets,
        "screens": output,
        # ``all_supplied_exact`` is deliberately distinct from full parity:
        # a two-screen reference must never be reported as a 45-preset match.
        "all_supplied_exact": all(item["exact"] for item in output.values()),
        "all_presets_exact": not missing_presets and all(item["exact"] for item in output.values()),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True, type=Path, help="JSON reference file with as_of_date and screens")
    parser.add_argument("--output", type=Path, help="Optional JSON report path; otherwise prints to stdout")
    parser.add_argument(
        "--require-full-exact-match", action="store_true",
        help="Exit non-zero unless the reference covers all 45 presets and every symbol set matches exactly.",
    )
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
    return 0 if not args.require_full_exact_match or report["all_presets_exact"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
