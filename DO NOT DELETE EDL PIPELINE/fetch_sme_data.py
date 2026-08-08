"""Fetch the NSE SME universe used to classify scanner securities."""

import sys

import requests

from pipeline_utils import save_json


OUTPUT_FILE = "sme_market_data.json"
API_URL = "https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi?functionName=getSmeData"
PAGE_URL = "https://www.nseindia.com/market-data/sme-market"


def fetch_sme_data():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": PAGE_URL,
    }
    session.get(PAGE_URL, headers=headers, timeout=30)
    response = session.get(API_URL, headers=headers, timeout=30)
    response.raise_for_status()
    rows = response.json().get("data", {}).get("data", [])
    cleaned = [
        {"Symbol": row.get("symbol"), "Series": row.get("series"),
         "Identifier": row.get("identifier"), "CompanyName": row.get("companyName")}
        for row in rows if row.get("symbol")
    ]
    if not cleaned:
        raise ValueError("NSE SME endpoint returned no securities")
    save_json(OUTPUT_FILE, cleaned)
    print(f"Successfully saved {len(cleaned)} NSE SME securities to {OUTPUT_FILE}")
    return True


if __name__ == "__main__":
    try:
        sys.exit(0 if fetch_sme_data() else 1)
    except Exception as exc:
        print(f"NSE SME fetch failed: {exc}")
        sys.exit(1)
