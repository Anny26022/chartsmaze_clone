"""Generate the versioned MBI/XP market-breadth artifacts."""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.breadth.config import load_methodology
from edl_pipeline.breadth.indices import generate_all_index_history
from edl_pipeline.breadth.pipeline import generate_market_breadth
from pipeline_utils import load_json


UNIVERSE_FILE = BASE_DIR / "dhan_data_response.json"
OHLCV_DIR = BASE_DIR / "ohlcv_data"
INDEX_FILE = BASE_DIR / "indices_ohlcv_data" / "NIFTY.csv"
INDEX_LIST_FILE = BASE_DIR / "all_indices_list.json"
INDICES_DIR = BASE_DIR / "indices_ohlcv_data"
METHODOLOGY_FILE = BASE_DIR / "breadth_methodology.json"
OUTPUT_FILE = BASE_DIR / "market_breadth_v2.json"
SNAPSHOT_FILE = BASE_DIR / "breadth_universe_snapshot.json"
ALL_INDICES_OUTPUT_FILE = BASE_DIR / "all_indices_history_v2.json"
MINIMUM_HISTORY_COVERAGE = 0.90


def history_coverage(processed, available):
    return processed / available if available else 0.0


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
    if not INDEX_LIST_FILE.exists():
        print("Error: all_indices_list.json is missing. Run fetch_all_indices.py first.")
        return 1
    if not INDICES_DIR.exists():
        print("Error: indices_ohlcv_data is missing. Run fetch_indices_ohlcv.py first.")
        return 1
    if not INDEX_FILE.exists():
        print("Error: NIFTY.csv is missing. Run fetch_indices_ohlcv.py first.")
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
    index_artifact = generate_all_index_history(
        index_rows=load_json(INDEX_LIST_FILE),
        indices_dir=INDICES_DIR,
        output_path=ALL_INDICES_OUTPUT_FILE,
        output_sessions=methodology.output_sessions,
        rounding_digits=methodology.rounding_digits,
        generated_at=artifact["generated_at"],
    )
    index_quality = index_artifact["quality"]
    print(
        f"Eligible: {snapshot['eligible_count']} | "
        f"Processed: {quality['processed_symbols']} | "
        f"Missing history: {quality['missing_history_count']} | "
        f"Dates: {quality['record_count']}"
    )

    equity_total = snapshot["eligible_count"]
    equity_coverage = history_coverage(quality["processed_symbols"], equity_total)
    index_total = index_quality["available_indices"]
    index_coverage = history_coverage(
        index_quality["processed_indices"],
        index_total,
    )
    if quality["record_count"] == 0 or equity_coverage < MINIMUM_HISTORY_COVERAGE:
        print(
            "Error: equity history coverage is incomplete "
            f"({equity_coverage:.1%}; required {MINIMUM_HISTORY_COVERAGE:.0%})."
        )
        return 1
    if index_coverage < MINIMUM_HISTORY_COVERAGE:
        print(
            "Error: index history coverage is incomplete "
            f"({index_coverage:.1%}; required {MINIMUM_HISTORY_COVERAGE:.0%})."
        )
        return 1
    print(f"Saved: {OUTPUT_FILE}")
    print(f"Saved: {SNAPSHOT_FILE}")
    print(
        f"Indices: {index_quality['processed_indices']}/"
        f"{index_quality['available_indices']} processed"
    )
    print(f"Saved: {ALL_INDICES_OUTPUT_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
