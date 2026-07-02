"""Compatibility wrapper for historical market breadth generation."""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.transforms.historical_breadth import calculate_historical_breadth


if __name__ == "__main__":
    sys.exit(0 if calculate_historical_breadth() else 1)
