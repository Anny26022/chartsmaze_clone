"""Compatibility wrapper for the fundamental analysis transform."""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.transforms.fundamentals import (
    analyze_all_stocks,
    analyze_stock,
    calculate_cagr,
    calculate_change,
    get_float,
    get_value_from_pipe_string,
)


if __name__ == "__main__":
    sys.exit(0 if analyze_all_stocks() else 1)
