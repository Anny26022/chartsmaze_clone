"""Versioned, point-in-time inputs for the local scanner."""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile


SCANNER_SNAPSHOT_FIELDS = (
    "symbol", "as_of_date", "close", "market_cap_crore", "free_float_percent",
    "pe_ratio", "latest_earnings_date", "sector", "industry", "circuit_limit",
    "listing_date", "listing_series", "delivery_series", "index_memberships",
    "qoq_percent_net_profit_latest", "yoy_percent_net_profit_latest",
    "qoq_percent_sales_latest", "yoy_percent_sales_latest",
    "qoq_percent_pbt_latest", "yoy_percent_pbt_latest",
    "qoq_percent_eps_latest", "yoy_percent_eps_latest",
)


def _write_gzip_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
    try:
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def build_snapshot(cache_dir: Path, stocks: list[dict], breadth: dict, fno_ban: dict, as_of_date: str) -> Path:
    """Persist only fields that influence scanner conditions for one session."""
    session = date.fromisoformat(as_of_date).isoformat()
    record = next((item for item in breadth.get("records", []) if item.get("date") == session), None)
    payload = {
        "schema_version": 1,
        "as_of_date": session,
        "stocks": [{field: stock.get(field) for field in SCANNER_SNAPSHOT_FIELDS} for stock in stocks if stock.get("symbol")],
        "breadth": record,
        "fno_ban": {
            "available": bool(fno_ban.get("available")),
            "trade_date": fno_ban.get("trade_date"),
            "symbols": fno_ban.get("symbols", []),
        },
    }
    path = cache_dir / f"{session}.json.gz"
    _write_gzip_json(path, payload)
    return path


def load_snapshot(cache_dir: Path, as_of_date: str | None) -> dict | None:
    if not as_of_date:
        return None
    path = cache_dir / f"{as_of_date}.json.gz"
    if not path.exists():
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return None
    return payload if payload.get("as_of_date") == as_of_date else None
