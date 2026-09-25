"""NSE daily full-bhavcopy delivery-data adapter."""

from __future__ import annotations

import csv
from datetime import datetime
from io import StringIO
from urllib.parse import urljoin

import requests


NSE_DAILY_REPORTS_URL = "https://www.nseindia.com/api/daily-reports?key=CM"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/all-reports",
}
DELIVERY_FILE_KEY = "CM-BHAVDATA-FULL"


def parse_nse_date(value: str) -> str:
    """Convert NSE's ``25-Sep-2026`` calendar date to ISO form."""
    return datetime.strptime(value.strip(), "%d-%b-%Y").date().isoformat()


def normalize_row(row: dict) -> dict | None:
    """Normalize one row from NSE's `sec_bhavdata_full` CSV."""
    row = {str(key).strip(): value for key, value in row.items()}
    symbol = str(row.get("SYMBOL") or "").strip().upper()
    session_date = row.get("DATE1")
    delivery_percent = row.get("DELIV_PER")
    if not symbol or not session_date or delivery_percent in (None, ""):
        return None
    try:
        return {
            "symbol": symbol,
            "series": str(row.get("SERIES") or "").strip().upper() or None,
            "date": parse_nse_date(session_date),
            "traded_quantity": int(float(row.get("TTL_TRD_QNTY") or 0)),
            "deliverable_quantity": int(float(row.get("DELIV_QTY") or 0)),
            "delivery_percent": float(delivery_percent),
            "source": "NSE daily full bhavcopy and security deliverable data",
        }
    except (TypeError, ValueError):
        return None


def latest_delivery_file(report_payload: dict) -> dict:
    """Find NSE's newest published Full Bhavcopy + Delivery CSV descriptor."""
    candidates = []
    for section in ("CurrentDay", "PreviousDay"):
        for item in report_payload.get(section, []):
            if item.get("fileKey") == DELIVERY_FILE_KEY and item.get("fileActlName"):
                candidates.append(item)
    if not candidates:
        raise RuntimeError("NSE daily reports did not list a full bhavcopy delivery file")
    return max(candidates, key=lambda item: parse_nse_date(item["tradingDate"]))


def fetch_latest_delivery_bhavcopy(session: requests.Session | None = None) -> tuple[dict, list[dict]]:
    """Download NSE's one-file full-universe delivery snapshot."""
    session = session or requests.Session()
    session.headers.update(NSE_HEADERS)
    reports = session.get(NSE_DAILY_REPORTS_URL, timeout=30)
    reports.raise_for_status()
    descriptor = latest_delivery_file(reports.json())
    url = urljoin(descriptor["filePath"].rstrip("/") + "/", descriptor["fileActlName"])
    response = session.get(url, timeout=60)
    response.raise_for_status()
    rows = csv.DictReader(StringIO(response.content.decode("utf-8-sig")))
    records = [item for row in rows if (item := normalize_row(row))]
    if not records:
        raise RuntimeError(f"NSE delivery file contained no usable records: {url}")
    return {
        "source": "NSE daily full bhavcopy and security deliverable data",
        "file_name": descriptor["fileActlName"],
        "file_url": url,
        "as_of_date": parse_nse_date(descriptor["tradingDate"]),
    }, records
