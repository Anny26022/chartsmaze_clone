"""
═══════════════════════════════════════════════════
  EDL Pipeline — Shared Utilities
  Centralizes common constants and helpers used
  across all fetch_*.py scripts.
═══════════════════════════════════════════════════
"""

import gzip
import json
import os
import random
from pathlib import Path

import requests

def _default_base_path():
    module_dir = Path(__file__).resolve().parent
    cwd = Path.cwd()
    if (cwd / "run_full_pipeline.py").exists() and (cwd / "pipeline_utils.py").exists():
        return cwd
    return module_dir


# ── Base Directory (all scripts live here) ──
BASE_PATH = Path(os.getenv("EDL_BASE_DIR", _default_base_path())).resolve()
BASE_DIR = str(BASE_PATH)
SCANX_FETCH_URL = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"

# ── User Agents (rotated to avoid detection) ──
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]


def get_headers(include_origin=False):
    """Return standard API headers with a random User-Agent."""
    h = {
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
    }
    if include_origin:
        h["Origin"] = "https://scanx.dhan.co"
        h["Referer"] = "https://scanx.dhan.co/"
    return h


def resolve_path(path):
    """Resolve a pipeline-relative path to an absolute Path."""
    path = Path(path)
    return path if path.is_absolute() else BASE_PATH / path


def ensure_dir(path):
    """Create a directory if it does not already exist."""
    resolve_path(path).mkdir(parents=True, exist_ok=True)


def load_json(path, default=None):
    """Load JSON from a pipeline-relative path."""
    resolved = resolve_path(path)
    try:
        with resolved.open("r") as f:
            return json.load(f)
    except FileNotFoundError:
        if default is not None:
            return default
        raise


def save_json(path, data, indent=4, ensure_ascii=True):
    """Write JSON to a pipeline-relative path and create parent dirs."""
    resolved = resolve_path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    with resolved.open("w") as f:
        json.dump(data, f, indent=indent, ensure_ascii=ensure_ascii)


def compress_file(src, dst, compresslevel=9):
    """Gzip one file and return raw/gz byte sizes."""
    src_path = resolve_path(src)
    dst_path = resolve_path(dst)
    if not src_path.exists():
        return 0, 0

    raw_size = src_path.stat().st_size
    with src_path.open("rb") as f_in, gzip.open(dst_path, "wb", compresslevel=compresslevel) as f_out:
        f_out.write(f_in.read())
    return raw_size, dst_path.stat().st_size


def chunked(items, size):
    """Yield fixed-size chunks from a list-like object."""
    for i in range(0, len(items), size):
        yield i, items[i:i + size]


def post_json(url, payload, include_origin=False, timeout=30):
    """POST JSON and return the decoded response."""
    response = requests.post(
        url,
        json=payload,
        headers=get_headers(include_origin=include_origin),
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def fetch_scanx_data(payload, timeout=30):
    """Fetch a list from the shared ScanX customscan endpoint."""
    data = post_json(SCANX_FETCH_URL, payload, include_origin=True, timeout=timeout)
    rows = data.get("data", [])
    return rows if isinstance(rows, list) else []
