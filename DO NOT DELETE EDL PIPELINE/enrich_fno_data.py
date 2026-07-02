"""
F&O Data Enrichment — Adds F&O flag, lot sizes, and next expiry
to the master all_stocks_fundamental_analysis.json.

Fetches lot sizes and expiry calendar from Dhan, then maps them
to the master ISIN map's FnoFlag field.
"""

import os
import sys
from datetime import datetime

from dhan_next_utils import get_build_id, get_next_data
from pipeline_utils import BASE_DIR, load_json, save_json

MASTER_JSON = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
MASTER_ISIN = os.path.join(BASE_DIR, "master_isin_map.json")
BUILD_ID_PAGE = "https://dhan.co/nse-fno-lot-size/"


def fetch_lot_sizes(build_id):
    """Fetch current F&O lot sizes. Returns {symbol: lot_size_current_month}."""
    lot_map = {}
    if not build_id:
        return lot_map

    data = get_next_data(build_id, "nse-fno-lot-size")
    instruments = data.get("pageProps", {}).get("listData", [])
    for item in instruments:
        sym = item.get("sym")
        fo_contracts = item.get("fo_dt", [])
        if sym and fo_contracts:
            lot_map[sym] = fo_contracts[0].get("ls")

    return lot_map


def fetch_next_expiry(build_id):
    """Fetch F&O expiry calendar. Returns {symbol: next_expiry_date}."""
    expiry_map = {}
    if not build_id:
        return expiry_map

    data = get_next_data(build_id, "fno-expiry-calendar")
    expiry_raw = data.get("pageProps", {}).get("expiryData", {}).get("data", [])
    today = datetime.now().strftime("%Y-%m-%d")

    for exchange_data in expiry_raw:
        for exp_group in exchange_data.get("exps", []):
            for item in exp_group.get("explst", []):
                sym = item.get("symbolName")
                exp_date = item.get("expdate")
                if sym and exp_date and exp_date >= today:
                    if sym not in expiry_map or exp_date < expiry_map[sym]:
                        expiry_map[sym] = exp_date

    return expiry_map


def main():
    # 1. Load master JSON
    if not os.path.exists(MASTER_JSON):
        print(f"Error: {MASTER_JSON} not found.")
        return False

    master_data = load_json(MASTER_JSON)

    # 2. Load ISIN map to get FnoFlag
    fno_symbols = set()
    if os.path.exists(MASTER_ISIN):
        for item in load_json(MASTER_ISIN):
            if item.get("FnoFlag") == 1 or item.get("FnoFlag") == "1":
                fno_symbols.add(item["Symbol"])

    print(f"Found {len(fno_symbols)} F&O eligible stocks from ISIN map.")

    # 3. Fetch lot sizes and expiry
    print("Fetching Dhan buildId...")
    build_id = get_build_id(BUILD_ID_PAGE)
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
    save_json(MASTER_JSON, master_data, ensure_ascii=False)

    print(f"Successfully enriched {enriched} F&O stocks in master JSON.")
    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
