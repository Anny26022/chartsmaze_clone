"""Run a daily trend screen against the locally cached OHLCV universe."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.trend import CONDITION_REGISTRY, evaluate_universe


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, help="JSON request with a non-empty conditions array")
    parser.add_argument("--output", type=Path, help="Write JSON result here; otherwise print it")
    parser.add_argument("--as-of-date", help="Use the latest session on or before YYYY-MM-DD")
    parser.add_argument("--delivery-history", type=Path, help="JSON delivery-history artifact with a records array")
    parser.add_argument("--include-non-matches", action="store_true")
    parser.add_argument("--list-conditions", action="store_true")
    args = parser.parse_args(argv)
    if args.list_conditions:
        print(json.dumps(CONDITION_REGISTRY, indent=2, sort_keys=True))
        return 0
    if not args.request:
        parser.error("--request is required unless --list-conditions is used")
    request = json.loads(args.request.read_text())
    conditions = request.get("conditions")
    if not isinstance(conditions, list) or not conditions:
        parser.error("request.conditions must be a non-empty array")
    delivery_history = {}
    delivery_path = args.delivery_history or (ROOT / "delivery_history_data")
    paths = sorted(delivery_path.glob("*.json")) if delivery_path.is_dir() else [delivery_path]
    for path in paths:
        if not path.exists():
            continue
        records = json.loads(path.read_text()).get("records", [])
        for item in records:
            if isinstance(item, dict) and item.get("symbol"):
                delivery_history.setdefault(item["symbol"], []).append(item)
    result = evaluate_universe(
        ROOT / "ohlcv_data",
        conditions,
        args.as_of_date or request.get("as_of_date"),
        args.include_non_matches or bool(request.get("include_non_matches")),
        delivery_history,
    )
    rendered = json.dumps(result, indent=2, allow_nan=False)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
