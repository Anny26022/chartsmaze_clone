"""Publish the dated ScanX ``sHp`` ownership series used by scanner snapshots."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from edl_pipeline.scanner.shareholding import observations_from_fundamentals
from pipeline_utils import BASE_DIR, load_json, save_json


def _latest_session(breadth: dict) -> str | None:
    dates = [str(row.get("date")) for row in breadth.get("records", []) if row.get("date")]
    return max(dates) if dates else None


def main() -> int:
    root = Path(BASE_DIR)
    fundamentals = load_json(root / "fundamental_data.json", default=[])
    master = load_json(root / "master_isin_map.json", default=[])
    breadth = load_json(root / "market_breadth_v2.json", default={})
    session = _latest_session(breadth)
    symbols = {str(row.get("Symbol") or "").upper() for row in master if isinstance(row, dict)}
    if not fundamentals or not symbols or not session:
        print("Cannot build shareholding history without filtered fundamentals, master symbols and a market session.")
        return 1
    records = observations_from_fundamentals(
        [row for row in fundamentals if str(row.get("Symbol") or "").upper() in symbols], session
    )
    if not records:
        print("No valid dated ScanX shareholding observations found.")
        return 1
    save_json(root / "shareholding_history.json", {
        "schema_version": 1,
        "source": "ScanX fundamental sHp ownership history",
        "universe": "NSE mainboard canonical universe",
        "as_of_date": session,
        "temporal_contract": (
            "period_end is the provider reporting-period label; observed_on is the first pipeline "
            "observation date. Filing dates are unavailable and are not inferred."
        ),
        "records": records,
    }, ensure_ascii=False)
    print(f"Published {len(records)} dated shareholding observations for {session}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
