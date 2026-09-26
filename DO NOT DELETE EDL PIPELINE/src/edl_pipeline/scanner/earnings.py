"""Point-in-time earnings observations for scanner conditions.

The fundamental provider exposes a *current* quarterly snapshot.  A scanner
must not relabel that snapshot as if it were known on an earlier screen date.
This module retains each observed quarterly result with the exchange filing
date that made it public, then selects only records available at the requested
date.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable


EARNINGS_FIELDS = (
    "latest_quarter",
    "latest_earnings_date",
    "earnings_report_type",
    "qoq_percent_net_profit_latest", "yoy_percent_net_profit_latest",
    "qoq_percent_sales_latest", "yoy_percent_sales_latest",
    "qoq_percent_pbt_latest", "yoy_percent_pbt_latest",
    "qoq_percent_eps_latest", "yoy_percent_eps_latest",
)


def _iso_date(value) -> str | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        return None


def observation_from_stock(stock: dict, observed_on: str) -> dict | None:
    """Return a provider observation only when it has a real filing date."""
    symbol = str(stock.get("symbol") or "").upper()
    announcement_date = _iso_date(stock.get("latest_earnings_date"))
    observed_on = _iso_date(observed_on)
    if not symbol or not announcement_date or not observed_on:
        return None
    row = {field: stock.get(field) for field in EARNINGS_FIELDS}
    row.update({
        "symbol": symbol,
        "announcement_date": announcement_date,
        "observed_on": observed_on,
        "source": "Dhan fundamental snapshot joined to exchange Financial Results filing date",
    })
    return row


def merge_observations(existing: Iterable[dict], stocks: Iterable[dict], observed_on: str) -> list[dict]:
    """Append one observation per symbol/filing/quarter without overwriting history."""
    rows = [dict(item) for item in existing if isinstance(item, dict)]
    keys = {
        (str(item.get("symbol") or "").upper(), item.get("announcement_date"), item.get("latest_quarter"))
        for item in rows
    }
    for stock in stocks:
        observation = observation_from_stock(stock, observed_on)
        if observation is None:
            continue
        key = (observation["symbol"], observation["announcement_date"], observation.get("latest_quarter"))
        if key not in keys:
            rows.append(observation)
            keys.add(key)
    return sorted(rows, key=lambda item: (
        str(item.get("symbol") or ""),
        str(item.get("announcement_date") or ""),
        str(item.get("observed_on") or ""),
    ))


def select_observation(observations: Iterable[dict], symbol: str, as_of_date: str) -> dict | None:
    """Select the newest filing publicly available on ``as_of_date``.

    ``observed_on`` is retained for provenance; filing date is the availability
    boundary because it is the event date surfaced by the exchange filing feed.
    """
    as_of = _iso_date(as_of_date)
    if not as_of:
        return None
    candidates = [
        item for item in observations
        if str(item.get("symbol") or "").upper() == str(symbol).upper()
        and (announcement := _iso_date(item.get("announcement_date"))) is not None
        and announcement <= as_of
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: (
        str(item.get("announcement_date")), str(item.get("observed_on")), str(item.get("latest_quarter") or ""),
    ))
