"""NSE security-wise delivery history adapter.

NSE's public archive is deliberately symbol-scoped.  This module models that
constraint instead of pretending that the endpoint is a bulk universe feed.
Callers must supply a bounded symbol list and persist the result before it is
merged into the scanner artifact.
"""

from __future__ import annotations

from datetime import datetime
import time
from typing import Sequence

import requests


NSE_REPORT_URL = "https://www.nseindia.com/report-detail/eq_security"
NSE_DELIVERY_URL = "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": NSE_REPORT_URL,
}


def parse_nse_date(value: str) -> str:
    """Convert NSE's ``25-Sep-2026`` date to an ISO calendar date."""
    return datetime.strptime(value, "%d-%b-%Y").date().isoformat()


def normalize_row(row: dict) -> dict | None:
    """Keep only the delivery fields that have a stable scanner meaning."""
    symbol = str(row.get("CH_SYMBOL") or "").strip().upper()
    session_date = row.get("mTIMESTAMP")
    delivery_percent = row.get("COP_DELIV_PERC")
    if not symbol or not session_date or delivery_percent is None:
        return None
    try:
        return {
            "symbol": symbol,
            "series": str(row.get("CH_SERIES") or "").strip().upper() or None,
            "date": parse_nse_date(session_date),
            "traded_quantity": int(float(row.get("CH_TOT_TRADED_QTY") or 0)),
            "deliverable_quantity": int(float(row.get("COP_DELIV_QTY") or 0)),
            "delivery_percent": float(delivery_percent),
            "source": "NSE security-wise price-volume-deliverable archive",
        }
    except (TypeError, ValueError):
        return None


def fetch_symbol_delivery_history(
    session: requests.Session,
    symbol: str,
    from_date: str,
    to_date: str,
    *,
    retries: int = 1,
) -> list[dict]:
    """Fetch one symbol's delivery history from NSE's current page API.

    Dates must be supplied as ``DD-MM-YYYY`` because that is the page API's
    public contract.  Retries are deliberately bounded; callers should cache
    successful downloads rather than repeatedly retrying an exchange site.
    """
    params = {
        "from": from_date,
        "to": to_date,
        "symbol": symbol.upper(),
        "type": "priceVolumeDeliverable",
        "series": "ALL",
    }
    error = None
    for attempt in range(retries + 1):
        try:
            response = session.get(NSE_DELIVERY_URL, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            rows = payload.get("data", []) if isinstance(payload, dict) else []
            return [item for row in rows if isinstance(row, dict) and (item := normalize_row(row))]
        except (requests.RequestException, ValueError) as exc:
            error = exc
            if attempt < retries:
                time.sleep(1 + attempt)
    raise RuntimeError(f"NSE delivery request failed for {symbol}: {error}")


def fetch_delivery_history(
    symbols: Sequence[str], from_date: str, to_date: str, *, delay_seconds: float = 0.25
) -> tuple[list[dict], list[dict]]:
    """Fetch a bounded symbol collection and report per-symbol failures."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    # Establish the same public report context used by NSE's browser UI.
    session.get(NSE_REPORT_URL, timeout=30)
    records, failures = [], []
    for index, symbol in enumerate(symbols):
        try:
            records.extend(fetch_symbol_delivery_history(session, symbol, from_date, to_date))
        except RuntimeError as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
        if delay_seconds and index + 1 < len(symbols):
            time.sleep(delay_seconds)
    return records, failures
