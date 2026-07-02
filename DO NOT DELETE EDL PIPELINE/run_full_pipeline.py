"""Compatibility wrapper for the package runner."""

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from edl_pipeline.config import env_bool  # re-exported for existing tests/users
from edl_pipeline.runner import main


if __name__ == "__main__":
    sys.exit(main())
