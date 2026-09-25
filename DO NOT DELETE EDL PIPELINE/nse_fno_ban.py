"""Official NSE daily F&O security-ban report adapter."""

from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urljoin

import requests

from nse_delivery import NSE_HEADERS


NSE_DAILY_FO_REPORTS_URL = "https://www.nseindia.com/api/daily-reports?key=FO"
FNO_BAN_FILE_KEY = "FO-SEC-BAN"


def _date(value):
    return datetime.strptime(value.strip(), "%d-%b-%Y").date().isoformat()


def latest_fno_ban_file(payload):
    """Return the newest report descriptor NSE publishes for the next session."""
    candidates = [
        item for section in ("CurrentDay", "PreviousDay")
        for item in payload.get(section, [])
        if item.get("fileKey") == FNO_BAN_FILE_KEY and item.get("fileActlName")
    ]
    if not candidates:
        raise RuntimeError("NSE daily FO reports did not list a security-ban CSV")
    return max(candidates, key=lambda item: item["fileActlName"])


def parse_report(text):
    """Parse NSE's numbered CSV and the *trade* date in its heading.

    NSE's daily-report index is a publication index, not necessarily the date to
    which the ban applies.  The CSV heading is the authoritative trade date.
    """
    heading = re.search(r"For Trade Date\s+(\d{1,2}-[A-Za-z]{3}-\d{4})", text, re.I)
    trade_date = _date(heading.group(1)) if heading else None
    symbols = []
    for line in text.splitlines():
        match = re.match(r"\s*\d+\s*,\s*([A-Za-z0-9&._-]+)\s*$", line)
        if match:
            symbols.append(match.group(1).upper())
    return trade_date, sorted(set(symbols))


def parse_symbols(text):
    """Compatibility helper for callers interested only in the ban symbols."""
    return parse_report(text)[1]


def fetch_latest_fno_ban(session=None):
    session = session or requests.Session()
    session.headers.update(NSE_HEADERS)
    reports = session.get(NSE_DAILY_FO_REPORTS_URL, timeout=30)
    reports.raise_for_status()
    descriptor = latest_fno_ban_file(reports.json())
    url = urljoin(descriptor["filePath"].rstrip("/") + "/", descriptor["fileActlName"])
    response = session.get(url, timeout=30)
    response.raise_for_status()
    trade_date, symbols = parse_report(response.content.decode("utf-8-sig"))
    return {
        "source": "NSE daily F&O Security in Ban Period report",
        "available": True,
        "file_name": descriptor["fileActlName"],
        "file_url": url,
        "published_on": _date(descriptor["tradingDate"]),
        "trade_date": trade_date,
        "symbols": symbols,
    }
