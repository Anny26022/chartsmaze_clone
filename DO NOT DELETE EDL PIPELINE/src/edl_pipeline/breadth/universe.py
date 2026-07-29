"""Build and audit the latest-snapshot breadth universe."""

from datetime import datetime, timezone
import math


def safe_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def evaluate_stock(row, methodology):
    symbol = str(row.get("Sym") or row.get("Symbol") or "").strip()
    isin = str(row.get("Isin") or row.get("ISIN") or "").strip()
    security_id = row.get("Sid") or row.get("SecurityId")
    price = safe_float(row.get("Ltp", row.get("Stock Price(₹)")))
    market_cap = safe_float(row.get("Mcap", row.get("Market Cap(Cr.)")))

    reasons = []
    if not symbol:
        reasons.append("missing_symbol")
    if not isin:
        reasons.append("missing_isin")
    if security_id in (None, ""):
        reasons.append("missing_security_id")
    if price is None:
        reasons.append("missing_price")
    elif price < methodology.minimum_price:
        reasons.append("price_below_minimum")
    if market_cap is None:
        reasons.append("missing_market_cap")
    elif market_cap <= methodology.minimum_market_cap_crore:
        reasons.append("market_cap_not_strictly_greater")

    return {
        "symbol": symbol,
        "name": row.get("DispSym") or row.get("Name") or symbol,
        "isin": isin,
        "security_id": security_id,
        "exchange": row.get("Exch", "NSE"),
        "segment": row.get("Seg", "E"),
        "instrument": row.get("Inst", "EQUITY"),
        "latest_price": price,
        "market_cap_crore": market_cap,
        "eligible": not reasons,
        "exclusion_reasons": reasons,
    }


def build_universe_snapshot(rows, methodology, generated_at=None):
    """Return a deterministic, deduplicated universe and full eligibility audit."""
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    evaluated = [evaluate_stock(row, methodology) for row in rows]

    by_symbol = {}
    duplicates = []
    for stock in evaluated:
        symbol = stock["symbol"]
        if not symbol:
            continue
        previous = by_symbol.get(symbol)
        if previous is None:
            by_symbol[symbol] = stock
            continue
        duplicates.append(symbol)
        previous_cap = previous["market_cap_crore"]
        current_cap = stock["market_cap_crore"]
        if current_cap is not None and (previous_cap is None or current_cap > previous_cap):
            by_symbol[symbol] = stock

    audited = sorted(by_symbol.values(), key=lambda item: item["symbol"])
    eligible = [item for item in audited if item["eligible"]]
    excluded = [item for item in audited if not item["eligible"]]

    return {
        "generated_at": generated_at,
        "methodology_version": methodology.version,
        "filters": {
            "latest_price_greater_than_or_equal_to": methodology.minimum_price,
            "market_cap_crore_strictly_greater_than": methodology.minimum_market_cap_crore,
        },
        "source_row_count": len(rows),
        "deduplicated_count": len(audited),
        "duplicate_symbols": sorted(set(duplicates)),
        "eligible_count": len(eligible),
        "excluded_count": len(excluded),
        "eligible": eligible,
        "excluded": excluded,
    }
