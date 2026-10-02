"""Small, deterministic compiler for the public Market-Lens-style query syntax.

The compiler deliberately produces the same expression-tree format as the
criteria builder.  It has no evaluator of its own, which keeps a saved query
and a builder-created screen on one calculation path.
"""

from __future__ import annotations

import re
from typing import Any


_OPERATORS = {">": "greater", ">=": "greater_or_equal", "<": "less", "<=": "less_or_equal", "=": "equal"}

# Names deliberately mirror the public query gallery.  Values are the stable
# internal field identifiers evaluated by ``field_comparison``.
FIELD_ALIASES = {
    "market cap (in cr)": "market_cap_crore", "market cap": "market_cap_crore",
    "price to earning (p/e)": "pe_ratio", "p/e": "pe_ratio", "pe ratio": "pe_ratio",
    "debt to equity": "debt_to_equity", "earning per share (eps)": "eps_ttm",
    "close price": "close", "current market price": "close", "open price": "open",
    "high price": "high", "low price": "low", "volume (in lakhs)": "volume_lakh",
    "20 dma": "sma_20", "50 dma": "sma_50", "200 dma": "sma_200",
    "52w high": "high_52w", "52w low": "low_52w",
    "return over % 1 month": "return_1m", "return over % 1 year": "return_1y",
    "return over % 3 years": "return_3y", "return over % 5 years": "return_5y",
    "return over % year to date": "return_ytd", "daily volatility": "daily_volatility",
    "annualized volatility": "annualized_volatility", "promoter holding (%)": "promoter_holding_percent",
    "public holding": "public_holding_percent", "number of shareholders": "number_of_shareholders",
    "dividend yield(%)": "dividend_yield_percent", "face value": "face_value",
    "total income (in lakhs)": "total_income_in_lakhs", "total expense (in lakhs)": "total_expense_in_lakhs",
    "profit before tax (in lakhs)": "profit_before_tax_in_lakhs", "total tax expenses (in lakhs)": "total_tax_expenses_in_lakhs",
    "net profit (in lakhs)": "net_profit_in_lakhs", "total equity (in lakhs)": "total_equity_in_lakhs",
    "total assets (in lakhs)": "total_assets_in_lakhs", "current assets (in lakhs)": "current_assets_in_lakhs",
    "current liabilities (in lakhs)": "current_liabilities_in_lakhs", "non-current liabilities (in lakhs)": "non_current_liabilities_in_lakhs",
    "operating cash flow (in lakhs)": "operating_cash_flow_in_lakhs", "investing cash flow (in lakhs)": "investing_cash_flow_in_lakhs",
    "net cash flow (in lakhs)": "net_cash_flow_in_lakhs",
    "total revenue (in lakhs)": "total_revenue_in_lakhs",
    "non-current assets (in lakhs)": "non_current_assets_in_lakhs",
    "total liabilities (in lakhs)": "total_liabilities_in_lakhs",
    "interest coverage": "interest_coverage", "vwap": "vwap",
    "dividend per share (dps)": "dividend_per_share_latest",
    "all time high": "all_time_high", "all time low": "all_time_low",
}


def _split(text: str, operator: str) -> list[str]:
    """Split an expression by a word operator outside parentheses."""
    depth = 0; pieces: list[str] = []; start = 0
    for match in re.finditer(r"\(|\)|\b" + operator + r"\b", text, flags=re.I):
        token = match.group(0)
        if token == "(": depth += 1
        elif token == ")": depth -= 1
        elif depth == 0:
            pieces.append(text[start:match.start()].strip()); start = match.end()
    return pieces + [text[start:].strip()]


def _strip_outer(text: str) -> str:
    while text.startswith("(") and text.endswith(")"):
        depth = 0; closes_early = False
        for index, char in enumerate(text):
            depth += char == "("; depth -= char == ")"
            if depth == 0 and index != len(text) - 1: closes_early = True; break
        if closes_early: break
        text = text[1:-1].strip()
    return text


def _operand(value: str) -> Any:
    value = value.strip()
    try: return float(value.replace(",", ""))
    except ValueError:
        field = FIELD_ALIASES.get(value.casefold())
        if not field: raise ValueError(f"Unsupported query field: {value!r}")
        return {"field": field}


def _arguments(text: str) -> list[str]:
    """Split comma-delimited function arguments while retaining quoted text."""
    values, start, depth, quote = [], 0, 0, None
    for index, char in enumerate(text):
        if char in {"'", '"'}:
            quote = None if quote == char else char if quote is None else quote
        elif quote is None:
            depth += char == "("
            depth -= char == ")"
            if char == "," and depth == 0:
                values.append(text[start:index].strip().strip("'\"")); start = index + 1
    values.append(text[start:].strip().strip("'\""))
    return values


def _function_condition(name: str, arguments: list[str], operator: str | None = None, value: str | None = None) -> dict:
    """Compile the public query functions to the existing condition contract."""
    key = re.sub(r"\s+", " ", name).strip().casefold()
    comparison = _OPERATORS.get(operator or ">=", "greater_or_equal")
    target = float(value.replace(",", "")) if value is not None else None
    if key == "adx":
        return {"type": "condition", "kind": "ADX", "params": {"period": int(arguments[0] or 14), "comparison": "ABOVE" if comparison.startswith("greater") else "BELOW", "value": target}}
    if key == "rvol":
        return {"type": "condition", "kind": "VOLUME_VS_AVG", "params": {"avgDays": int(arguments[0] or 20), "multiple": target, "withinDays": 1}}
    if key == "adr":
        return {"type": "condition", "kind": "ADR_PCT", "params": {"lookbackDays": int(arguments[0] or 14), "comparison": "ABOVE" if comparison.startswith("greater") else "BELOW", "pct": target}}
    if key == "volume trend":
        return {"type": "condition", "kind": "AVG_VOLUME_RATIO", "params": {"recentDays": int(arguments[0]), "baseDays": int(arguments[1]), "comparison": "ABOVE" if comparison.startswith("greater") else "BELOW", "ratio": target}}
    if key == "earnings growth":
        return {"type": "condition", "kind": "EARNINGS_GROWTH", "params": {"metric": arguments[0], "basis": arguments[1], "comparison": "ABOVE" if comparison.startswith("greater") else "BELOW", "value": target}}
    if key == "days since earnings":
        return {"type": "condition", "kind": "DAYS_SINCE_EARNINGS", "params": {"comparison": "ABOVE" if comparison.startswith("greater") else "BELOW", "days": target}}
    if key == "ma stack":
        periods = [int(part.strip()) for part in arguments[0].split(",")]
        return {"type": "condition", "kind": "MA_STACK", "params": {"periods": periods, "maType": arguments[1], "priceAbove": arguments[2].lower() == "true"}}
    if key == "ma slope":
        return {"type": "condition", "kind": "MA_SLOPE", "params": {"period": int(arguments[0]), "maType": arguments[1], "window": int(arguments[2]), "comparison": "ABOVE" if comparison.startswith("greater") else "BELOW", "minChangePct": target}}
    raise ValueError(f"Unsupported query function: {name!r}")


def _leaf(text: str) -> dict:
    function = re.match(r"^(.+?)\((.*)\)\s*(>=|<=|>|<|=)\s*(.+)$", text.strip())
    field_match = re.match(r"^(.+?)\s*(>=|<=|>|<|=)\s*(.+)$", text.strip())
    known_field = field_match and field_match.group(1).strip().casefold() in FIELD_ALIASES
    if function and not known_field:
        name, arguments, operator, value = function.groups()
        return _function_condition(name, _arguments(arguments), operator, value)
    bare_function = re.match(r"^(.+?)\((.*)\)$", text.strip())
    if bare_function and not field_match:
        return _function_condition(bare_function.group(1), _arguments(bare_function.group(2)))
    match = field_match
    if not match: raise ValueError(f"Expected a comparison in query clause: {text!r}")
    left, operator, right = match.groups()
    left_value = _operand(left)
    if not isinstance(left_value, dict): raise ValueError("The left side of a query comparison must be a field.")
    return {"type": "condition", "condition": "field_comparison", "field": left_value["field"], "comparison": _OPERATORS[operator], "value": _operand(right)}


def compile_query(query: str) -> dict:
    """Compile ``Field > number AND …`` into a scanner expression tree."""
    text = _strip_outer(str(query or "").strip())
    if not text: raise ValueError("Query is empty.")
    for operator in ("OR", "AND"):
        parts = _split(text, operator)
        if len(parts) > 1:
            return {"type": "group", "op": operator, "children": [compile_query(part) for part in parts]}
    return _leaf(text)
