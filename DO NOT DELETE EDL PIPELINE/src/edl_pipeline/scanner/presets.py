"""Versioned JournalToday-compatible screener preset library.

The public client bundle publishes named preset metadata and a declarative
condition-expression tree.  This module vendors that *data contract* at a
specific version so a Chartsmaze scan is offline, reproducible and never
depends on a third-party client bundle at runtime.

The library describes combinations of local conditions; it does not claim
proprietary backend parity for complex pattern primitives.
"""

from __future__ import annotations

import base64
from copy import deepcopy
from functools import lru_cache
import gzip
import json
from typing import Any, Mapping


SOURCE = {
    "site": "journaltoday.org",
    "bundle": "StockScreenerPage-Bd2hD4Xi.js",
    "bundle_version": "v1.0.39",
    "retrieved_at": "2026-09-26",
    "note": "Preset structure and defaults extracted from the public client bundle; evaluation uses Chartsmaze local condition semantics.",
}

# gzip+base64 keeps the vendored, JSON-compatible source record compact in the
# Python package. ``export_preset_library`` writes it back as readable JSON.
_ENCODED_LIBRARY = (
    "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"
)


@lru_cache(maxsize=1)
def _library() -> dict[str, Any]:
    decoded = gzip.decompress(base64.b64decode(_ENCODED_LIBRARY)).decode("utf-8")
    library = json.loads(decoded)
    presets = library.get("presets")
    if not isinstance(presets, list) or len(presets) != 45:
        raise ValueError("Invalid vendored screener preset library.")
    if library.get("schema_version") != 1:
        raise ValueError("Unsupported screener preset-library schema.")
    return library


def load_preset_library() -> dict[str, Any]:
    """Return a defensive copy of the complete, versioned preset library."""
    return deepcopy(_library())


def list_presets() -> list[dict[str, Any]]:
    """Return display metadata without duplicating the full expression tree."""
    return [
        {
            "id": preset["id"], "name": preset["name"],
            "category": preset["category"], "horizon": preset["horizon"],
            "description": preset["description"], "rule_count": len(preset["rules"]),
        }
        for preset in _library()["presets"]
    ]


def get_preset(preset_id: str) -> dict[str, Any]:
    """Look up one preset by its stable ``lib-*`` identifier (or name)."""
    query = str(preset_id).strip().casefold()
    for preset in _library()["presets"]:
        if preset["id"].casefold() == query or preset["name"].casefold() == query:
            return deepcopy(preset)
    available = ", ".join(preset["id"] for preset in _library()["presets"])
    raise ValueError(f"Unknown screener preset {preset_id!r}. Available IDs: {available}")


def validate_preset_library(condition_registry: Mapping[str, Any]) -> None:
    """Fail fast if a vendored expression refers to an unsupported condition."""
    from .context import normalize_condition_spec

    def visit(node: Mapping[str, Any]) -> None:
        if node.get("type") == "group":
            for child in node.get("children", []):
                visit(child)
            return
        condition = normalize_condition_spec(dict(node)).get("condition")
        if condition not in condition_registry:
            raise ValueError(f"Preset uses unsupported condition: {condition!r}")

    ids = set()
    for preset in _library()["presets"]:
        if preset["id"] in ids:
            raise ValueError(f"Duplicate screener preset ID: {preset['id']}")
        ids.add(preset["id"])
        visit(preset["expression"])


def export_preset_library(path) -> None:
    """Write the human-readable source record for review or API publication."""
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(_library(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
