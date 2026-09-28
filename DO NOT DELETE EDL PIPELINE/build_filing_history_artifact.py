"""Publish the persistent, normalized ScanX filing-history cache."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pipeline_utils import BASE_DIR, load_json, save_json


def main() -> int:
    root = Path(BASE_DIR)
    cache = load_json(root / "filing_history_data" / "filing_history.json", default={})
    symbols = cache.get("symbols") if isinstance(cache, dict) else None
    if not isinstance(symbols, dict) or not symbols:
        print("Cannot publish filing history without a populated persistent cache.")
        return 1
    records = [{"symbol": symbol, **entry} for symbol, entry in symbols.items() if isinstance(entry, dict)]
    records.sort(key=lambda item: item["symbol"])
    if not records or not any(item.get("filings") for item in records):
        print("No usable filing-history records found.")
        return 1
    complete = sum(bool(item.get("lodr_backfill_complete")) for item in records)
    save_json(root / "filing_history.json", {
        "schema_version": 1,
        "source": "ScanX static company_filings and LODR endpoints",
        "updated_at": cache.get("updated_at"),
        "coverage": {"symbols": len(records), "lodr_backfill_complete": complete, "lodr_backfill_pending": len(records) - complete},
        "records": records,
    }, ensure_ascii=False)
    print(f"Published filing history for {len(records)} symbols ({complete} LODR backfills complete).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
