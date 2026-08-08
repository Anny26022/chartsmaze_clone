"""Compatibility wrapper for market breadth transforms."""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.transforms.market_breadth import generate_analytics, main


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
