"""Publish a mainboard IPO/listing catalogue from authoritative NSE listing dates.

This deliberately provides listing-based screening only.  Issue terms,
subscription figures and anchor lock-ins are not inferred from OHLCV or a
company snapshot; their absence is advertised in the artifact instead.
"""

from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path

from pipeline_utils import BASE_DIR, load_json, save_json


def _clean_row(row):
    return {str(key).strip(): value for key, value in row.items()}


def _listing_date(value):
    if not value:
        return None
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        pass
    for pattern in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            pass
    return None


def _reference_session(root: Path):
    path = root / "indices_ohlcv_data" / "NIFTY.csv"
    if not path.exists():
        return None
    with path.open(newline="", encoding="utf-8") as handle:
        dates = [_listing_date(row.get("Date")) for row in csv.DictReader(handle)]
    return max((item for item in dates if item), default=None)


def build_ipo_catalog(stocks, equity_rows, as_of_date):
    """Join NSE EQ listings to scanner-eligible canonical securities."""
    canonical = {
        str(item.get("symbol", "")).upper(): item
        for item in stocks
        if item.get("symbol") and item.get("default_screener_eligible") is not False
    }
    records, pending = [], []
    for raw in equity_rows:
        row = _clean_row(raw)
        symbol = str(row.get("SYMBOL") or "").strip().upper()
        listed = _listing_date(row.get("DATE OF LISTING"))
        if not symbol or str(row.get("SERIES") or "").strip().upper() != "EQ" or listed is None:
            continue
        if as_of_date and listed > as_of_date:
            continue
        stock = canonical.get(symbol)
        if stock is None:
            pending.append({"symbol": symbol, "listing_date": listed.isoformat(), "reason": "pending_canonical_enrichment"})
            continue
        records.append({
            "symbol": symbol, "name": stock.get("name"), "isin": stock.get("isin"),
            "security_id": stock.get("security_id"), "listing_date": listed.isoformat(),
            "listing_age_calendar_days": (as_of_date - listed).days if as_of_date else None,
            "close": stock.get("close"), "market_cap_crore": stock.get("market_cap_crore"),
            "sector": stock.get("sector"), "industry": stock.get("industry"),
            "rupee_volume": stock.get("rupee_volume"), "delivery_percent": stock.get("delivery_percent"),
            # Explicit nulls prevent clients from treating unavailable IPO
            # terms as zero or trying to derive them from market prices.
            "issue_price": None, "offer_structure": None,
            "retail_subscription_multiple": None, "institutional_subscription_multiple": None,
            "anchor_lock_in_end": None,
        })
    return sorted(records, key=lambda item: (item["listing_date"], item["symbol"]), reverse=True), sorted(pending, key=lambda item: item["symbol"])


def main():
    root = Path(BASE_DIR)
    listing_path = root / "nse_equity_list.csv"
    if not listing_path.exists():
        print("NSE listing-date CSV is unavailable.")
        return 1
    stocks = load_json(root / "all_stocks_fundamental_analysis.json", default=[])
    as_of = _reference_session(root)
    if not stocks:
        print("Canonical stock artifact is unavailable.")
        return 1
    if as_of is None:
        print("NIFTY reference session is unavailable.")
        return 1
    with listing_path.open(newline="", encoding="utf-8-sig") as handle:
        records, pending = build_ipo_catalog(stocks, csv.DictReader(handle), as_of)
    save_json(root / "ipo_screener.json", {
        "schema_version": 1,
        "source": "NSE EQUITY_L.csv joined to canonical mainboard scanner universe",
        "as_of_date": as_of.isoformat(),
        "records": records,
        "pending_canonical_enrichment": pending,
        "capabilities": {
            "listing_date_filter": True,
            "normal_scanner_conditions": True,
            "issue_terms": False,
            "subscription_multiples": False,
            "anchor_lock_in": False,
        },
    }, ensure_ascii=False)
    print(f"Published {len(records)} canonical EQ listings; {len(pending)} await enrichment.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
