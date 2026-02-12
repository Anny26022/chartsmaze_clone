"""
F&O Data Enrichment — Adds F&O flag, lot sizes, and next expiry
to the master all_stocks_fundamental_analysis.json.

Fetches lot sizes and expiry calendar from Dhan, then maps them
to the master ISIN map's FnoFlag field.
"""

import json
import os
import re
import requests
from pipeline_utils import BASE_DIR, get_headers

MASTER_JSON = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
MASTER_ISIN = os.path.join(BASE_DIR, "master_isin_map.json")


def get_build_id():
    """Dynamically fetch the Next.js buildId from a Dhan page."""
    url = "https://dhan.co/nse-fno-lot-size/"
    try:
        response = requests.get(url, headers={"User-Agent": get_headers()["User-Agent"]}, timeout=10)
        match = re.search(r'"buildId":"([^"]+)"', response.text)
        return match.group(1) if match else None
    except:
        return None


def fetch_lot_sizes(build_id):
    """Fetch current F&O lot sizes. Returns {symbol: lot_size_current_month}."""
    lot_map = {}
    if not build_id:
        return lot_map

    url = f"https://dhan.co/_next/data/{build_id}/nse-fno-lot-size.json"
    headers = {"User-Agent": get_headers()["User-Agent"]}

    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            instruments = data.get("pageProps", {}).get("listData", [])
            for item in instruments:
                sym = item.get("sym")
                fo_contracts = item.get("fo_dt", [])
                if sym and fo_contracts:
                    # First contract = current month lot size
                    lot_map[sym] = fo_contracts[0].get("ls")
    except:
        pass

    return lot_map


def fetch_next_expiry(build_id):
    """Fetch F&O expiry calendar. Returns {symbol: next_expiry_date}."""
    expiry_map = {}
    if not build_id:
        return expiry_map

    url = f"https://dhan.co/_next/data/{build_id}/fno-expiry-calendar.json"
    headers = {"User-Agent": get_headers()["User-Agent"]}

    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            expiry_raw = data.get("pageProps", {}).get("expiryData", {}).get("data", [])

            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")

            for exchange_data in expiry_raw:
                for exp_group in exchange_data.get("exps", []):
                    for item in exp_group.get("explst", []):
                        sym = item.get("symbolName")
                        exp_date = item.get("expdate")
                        if sym and exp_date and exp_date >= today:
                            # Keep the nearest future expiry
                            if sym not in expiry_map or exp_date < expiry_map[sym]:
                                expiry_map[sym] = exp_date
    except:
        pass

    return expiry_map


def main():
    # 1. Load master JSON
    if not os.path.exists(MASTER_JSON):
        print(f"Error: {MASTER_JSON} not found.")
        return

    with open(MASTER_JSON, "r") as f:
        master_data = json.load(f)

    # 2. Load ISIN map to get FnoFlag
    fno_symbols = set()
    if os.path.exists(MASTER_ISIN):
        with open(MASTER_ISIN, "r") as f:
            isin_map = json.load(f)
            for item in isin_map:
                if item.get("FnoFlag") == 1 or item.get("FnoFlag") == "1":
                    fno_symbols.add(item["Symbol"])

    print(f"Found {len(fno_symbols)} F&O eligible stocks from ISIN map.")

    # 3. Fetch lot sizes and expiry
    print("Fetching Dhan buildId...")
    build_id = get_build_id()
    print(f"  BuildId: {build_id}")

    print("Fetching F&O lot sizes...")
    lot_map = fetch_lot_sizes(build_id)
    print(f"  Got lot sizes for {len(lot_map)} instruments.")

    print("Fetching F&O expiry calendar...")
    expiry_map = fetch_next_expiry(build_id)
    print(f"  Got expiry dates for {len(expiry_map)} instruments.")

    # 4. Enrich master JSON
    enriched = 0
    for stock in master_data:
        sym = stock.get("Symbol")

        if sym in fno_symbols:
            stock["F&O"] = "Yes"
            stock["Lot Size"] = lot_map.get(sym, "N/A")
            stock["Next Expiry"] = expiry_map.get(sym, "N/A")
            enriched += 1
        else:
            stock["F&O"] = "No"
            stock["Lot Size"] = "N/A"
            stock["Next Expiry"] = "N/A"

    # 5. Save
    with open(MASTER_JSON, "w") as f:
        json.dump(master_data, f, indent=4, ensure_ascii=False)

    print(f"Successfully enriched {enriched} F&O stocks in master JSON.")


if __name__ == "__main__":
    main()
