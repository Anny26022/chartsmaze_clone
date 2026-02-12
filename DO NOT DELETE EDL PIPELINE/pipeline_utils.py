"""
═══════════════════════════════════════════════════
  EDL Pipeline — Shared Utilities
  Centralizes common constants and helpers used
  across all fetch_*.py scripts.
═══════════════════════════════════════════════════
"""

import os
import random

# ── Base Directory (all scripts live here) ──
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
