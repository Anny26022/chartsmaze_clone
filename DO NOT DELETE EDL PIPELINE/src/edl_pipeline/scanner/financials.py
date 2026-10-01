"""Dated statement selection for scanner P/E and quarterly growth."""
from datetime import date
import math


def statement_rows(context, stock, spec, as_of):
    rows = context.get("financial_history", {}).get(stock.get("symbol"), [])
    # The ledger contains current numeric revisions. Historical use requires
    # an explicit observation date; a filing timestamp alone is insufficient.
    current = context.get("financial_history_as_of") == as_of.isoformat()
    eligible = [r for r in rows if str(r.get("filing_date", ""))[:10] <= as_of.isoformat()
                and (current or (r.get("observed_on") and str(r["observed_on"])[:10] <= as_of.isoformat()))]
    requested = str(spec.get("report_type", "PREFER_CONSOLIDATED")).upper()
    if requested not in {"PREFER_CONSOLIDATED", "CONSOLIDATED", "STANDALONE"}:
        raise ValueError("Unsupported financial report type")
    types = [requested] if requested != "PREFER_CONSOLIDATED" else ["CONSOLIDATED", "STANDALONE"]
    for report_type in types:
        by_quarter = {}
        for row in sorted(eligible, key=lambda r: str(r.get("filing_date", ""))):
            if row.get("report_type") == report_type:
                by_quarter[row["quarter_end"]] = row
        if by_quarter:
            return sorted(by_quarter.values(), key=lambda r: r["quarter_end"])
    return []


def quarter_index(value):
    d = date.fromisoformat(value)
    return d.year * 4 + (d.month - 1) // 3


def finite_number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def financial_value(context, stock, spec, as_of, market_cap):
    """Return value, details, reason; never fill gaps with another statement."""
    rows = statement_rows(context, stock, spec, as_of)
    if not rows:
        return None, {}, "dated_financial_statement_unavailable"
    latest = rows[-1]
    details = {"report_type": latest["report_type"], "filing_date": latest["filing_date"], "quarter_end": latest["quarter_end"]}
    if spec["condition"] == "pe_ratio":
        selected = rows[-4:]
        quarters = [quarter_index(r["quarter_end"]) for r in selected]
        profits = [finite_number(r.get("net_profit")) for r in selected]
        if len(selected) != 4 or quarters != list(range(quarters[-1] - 3, quarters[-1] + 1)) or any(v is None for v in profits):
            return None, details, "four_consecutive_announced_quarters_unavailable"
        total = sum(profits)
        if total <= 0 or market_cap is None:
            return None, details, "positive_ttm_profit_or_market_cap_unavailable"
        return market_cap / total, {**details, "ttm_net_profit_crore": total}, None
    metric = {"revenue": "revenue", "net_profit": "net_profit", "pbt": "profit_before_tax", "eps": "eps", "opm": "opm"}.get(str(spec.get("metric", "net_profit")).lower())
    if metric is None:
        raise ValueError("Unsupported earnings metric")
    basis = str(spec.get("basis", "yoy")).lower()
    if basis not in {"qoq", "yoy"}:
        raise ValueError("Unsupported earnings basis")
    prior_quarter = quarter_index(latest["quarter_end"]) - (1 if basis == "qoq" else 4)
    prior = next((r for r in rows if quarter_index(r["quarter_end"]) == prior_quarter), {})
    value, base = finite_number(latest.get(metric)), finite_number(prior.get(metric))
    age = (as_of - date.fromisoformat(str(latest["filing_date"])[:10])).days
    if age > int(spec.get("maximum_filing_age_days", 200)):
        return None, details, "earnings_filing_too_old"
    if value is None or base in {None, 0}:
        return None, details, "comparison_quarter_unavailable"
    return (value - base) / abs(base) * 100, {**details, "filing_age_days": age, "metric": metric, "basis": basis}, None
