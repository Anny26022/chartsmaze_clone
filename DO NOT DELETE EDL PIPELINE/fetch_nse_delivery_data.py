"""Fetch a deliberately bounded NSE delivery-history batch.

The NSE archive endpoint is symbol-scoped.  Use this command in a scheduled,
cached ingestion job (or with an appropriately licensed bulk history source),
then run the normal EDL pipeline to merge ``nse_delivery_data.json``.
"""

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from nse_delivery import fetch_delivery_history
from pipeline_utils import BASE_DIR, load_json, save_json


OUTPUT_FILE = Path(BASE_DIR) / "nse_delivery_data.json"


def nse_date(value: str) -> str:
    return datetime.strptime(value, "%Y-%m-%d").strftime("%d-%m-%Y")


def default_dates() -> tuple[str, str]:
    now = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    return nse_date((now - timedelta(days=7)).isoformat()), nse_date(now.isoformat())


def parse_args():
    start, end = default_dates()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols-file", required=True, help="JSON list of symbols, or master ISIN map JSON")
    parser.add_argument("--from-date", default=start, help="DD-MM-YYYY")
    parser.add_argument("--to-date", default=end, help="DD-MM-YYYY")
    parser.add_argument("--max-symbols", type=int, default=100, help="Safety cap; use an authorised bulk source for full history")
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    return parser.parse_args()


def load_symbols(path: str) -> list[str]:
    raw = load_json(path)
    values = raw if isinstance(raw, list) else []
    symbols = [item if isinstance(item, str) else item.get("Symbol") for item in values if isinstance(item, (str, dict))]
    return sorted({str(symbol).strip().upper() for symbol in symbols if symbol})


def main():
    args = parse_args()
    symbols = load_symbols(args.symbols_file)
    if len(symbols) > args.max_symbols:
        print(f"Refusing {len(symbols)} symbols: --max-symbols is {args.max_symbols}. Use an authorised bulk source for universe backfill.")
        return False
    records, failures = fetch_delivery_history(symbols, args.from_date, args.to_date, delay_seconds=args.delay_seconds)
    save_json(OUTPUT_FILE, {
        "source": "NSE security-wise price-volume-deliverable archive",
        "from_date": args.from_date,
        "to_date": args.to_date,
        "records": records,
        "failures": failures,
    }, ensure_ascii=False)
    print(f"Saved {len(records)} delivery rows for {len(symbols)} symbols; {len(failures)} requests failed.")
    return not failures


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
