"""Generate the versioned MBI/XP market-breadth artifacts."""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.breadth.config import load_methodology
from edl_pipeline.breadth.pipeline import generate_market_breadth
from pipeline_utils import load_json


UNIVERSE_FILE = BASE_DIR / "dhan_data_response.json"
OHLCV_DIR = BASE_DIR / "ohlcv_data"
INDEX_FILE = BASE_DIR / "indices_ohlcv_data" / "NIFTY.csv"
METHODOLOGY_FILE = BASE_DIR / "breadth_methodology.json"
OUTPUT_FILE = BASE_DIR / "market_breadth_v2.json"
SNAPSHOT_FILE = BASE_DIR / "breadth_universe_snapshot.json"


def main():
    if not UNIVERSE_FILE.exists():
        print("Error: dhan_data_response.json is missing. Run fetch_dhan_data.py first.")
        return 1
    if not OHLCV_DIR.exists():
        print("Error: ohlcv_data is missing. Run fetch_all_ohlcv.py first.")
        return 1
    if not METHODOLOGY_FILE.exists():
        print("Error: breadth_methodology.json is missing.")
        return 1

    methodology = load_methodology(METHODOLOGY_FILE)
    universe_rows = load_json(UNIVERSE_FILE)
    print(
        "Building MBI/XP breadth with "
        f"LTP >= {methodology.minimum_price:g} and "
        f"Mcap > {methodology.minimum_market_cap_crore:g} crore..."
    )

    artifact, snapshot = generate_market_breadth(
        universe_rows=universe_rows,
        ohlcv_dir=OHLCV_DIR,
        index_csv=INDEX_FILE,
        methodology=methodology,
        output_path=OUTPUT_FILE,
        snapshot_path=SNAPSHOT_FILE,
    )
    quality = artifact["quality"]
    print(
        f"Eligible: {snapshot['eligible_count']} | "
        f"Processed: {quality['processed_symbols']} | "
        f"Missing history: {quality['missing_history_count']} | "
        f"Dates: {quality['record_count']}"
    )

    if quality["processed_symbols"] == 0 or quality["record_count"] == 0:
        print("Error: breadth artifacts contain no processed symbols or dates.")
        return 1
    print(f"Saved: {OUTPUT_FILE}")
    print(f"Saved: {SNAPSHOT_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
