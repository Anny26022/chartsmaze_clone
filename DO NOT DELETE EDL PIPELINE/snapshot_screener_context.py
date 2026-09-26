"""Retain a dated scanner-context snapshot after each successful refresh."""

from __future__ import annotations

import gzip
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.earnings import merge_observations
from edl_pipeline.scanner.history import build_snapshot
from pipeline_utils import BASE_DIR, load_json, save_json


def _read_artifact(root: Path, stem: str, default=None):
    raw = root / f"{stem}.json"
    if raw.exists():
        return load_json(raw, default=default)
    compressed = root / f"{stem}.json.gz"
    if not compressed.exists():
        return default
    with gzip.open(compressed, "rt", encoding="utf-8") as handle:
        import json
        return json.load(handle)


def main():
    root = Path(BASE_DIR)
    stocks = _read_artifact(root, "all_stocks_fundamental_analysis", default=[])
    breadth = _read_artifact(root, "market_breadth_v2", default={})
    fno = _read_artifact(root, "nse_fno_ban", default={})
    records = breadth.get("records", [])
    if not stocks or not records:
        print("Cannot snapshot scanner context without canonical stocks and breadth.")
        return 1
    session = records[-1].get("date")
    if not session:
        print("Cannot snapshot scanner context without a breadth session date.")
        return 1
    # Kept inside the persistent scanner-history cache, which publication
    # mounts into the staging refresh rather than deleting as an intermediate.
    ledger_path = root / "scanner_history_data" / "earnings_observations.json"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger = load_json(ledger_path, default={})
    observations = merge_observations(ledger.get("observations", []), stocks, session)
    save_json(ledger_path, {
        "schema_version": 1,
        "source": "Dhan fundamental snapshot joined to exchange Financial Results filing date",
        "observations": observations,
    }, ensure_ascii=False)
    path = build_snapshot(root / "scanner_history_data", stocks, breadth, fno, session, observations)
    print(f"Saved scanner context snapshot: {path.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
