"""Compatibility wrapper for event enrichment."""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.transforms.events import map_refined_events


if __name__ == "__main__":
    sys.exit(0 if map_refined_events() else 1)
