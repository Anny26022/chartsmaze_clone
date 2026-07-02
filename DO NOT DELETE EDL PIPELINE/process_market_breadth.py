"""Compatibility wrapper for market breadth transforms."""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.transforms.market_breadth import (
    calculate_1m_rs,
    calculate_3m_rs,
    calculate_current_rs,
    calculate_historical_returns,
    calculate_returns_as_of_date,
    generate_analytics,
    load_benchmark_returns,
    load_historical_returns,
    main,
    process_stock_ohlcv_full,
)


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
