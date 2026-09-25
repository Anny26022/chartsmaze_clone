"""
F&O Data Enrichment — Adds F&O flag, lot sizes, and next expiry
to the master all_stocks_fundamental_analysis.json.

Fetches lot sizes and expiry calendar from Dhan, then maps them
to the master ISIN map's FnoFlag field.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
import re

from dhan_next_utils import get_build_id, get_next_data
from pipeline_utils import BASE_DIR, load_json, save_json

MASTER_JSON = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
MASTER_ISIN = os.path.join(BASE_DIR, "master_isin_map.json")
BUILD_ID_PAGE = "https://dhan.co/nse-fno-lot-size/"


def normalized_symbol(value):
    """Compare provider symbols despite punctuation, suffix, and case changes."""
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper()).removesuffix("EQ")


def normalized_security_id(value):
    return str(value).strip() if value not in (None, "") else None


def ist_today():
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).date().isoformat()


def fetch_lot_sizes(build_id):
    """Fetch current F&O lot sizes. Returns {symbol: lot_size_current_month}."""
    lot_map = {}
    if not build_id:
        return lot_map

    data = get_next_data(build_id, "nse-fno-lot-size")
    instruments = data.get("pageProps", {}).get("listData", [])
    for item in instruments:
        sym = normalized_symbol(item.get("sym"))
        fo_contracts = item.get("fo_dt", [])
        if sym and fo_contracts:
            lot_map[sym] = fo_contracts[0].get("ls")

    return lot_map


def fetch_next_expiry(build_id):
    """Fetch expiry dates keyed by both Dhan security ID and normalized symbol."""
    by_symbol = {}
    by_security_id = {}
    if not build_id:
        return by_symbol, by_security_id

    data = get_next_data(build_id, "fno-expiry-calendar")
    expiry_raw = data.get("pageProps", {}).get("expiryData", {}).get("data", [])
    today = ist_today()

    for exchange_data in expiry_raw:
        for exp_group in exchange_data.get("exps", []):
            for item in exp_group.get("explst", []):
                sym = normalized_symbol(item.get("symbolName"))
                security_id = normalized_security_id(item.get("underlyingSecID"))
                exp_date = item.get("expdate")
                if exp_date and exp_date >= today:
                    if sym and (sym not in by_symbol or exp_date < by_symbol[sym]):
                        by_symbol[sym] = exp_date
                    if security_id and (security_id not in by_security_id or exp_date < by_security_id[security_id]):
                        by_security_id[security_id] = exp_date

    return by_symbol, by_security_id


def lookup_expiry(by_symbol, by_security_id, symbol, security_id):
    return by_security_id.get(normalized_security_id(security_id)) or by_symbol.get(normalized_symbol(symbol))


def main():
    # 1. Load master JSON
    if not os.path.exists(MASTER_JSON):
        print(f"Error: {MASTER_JSON} not found.")
        return False

    master_data = load_json(MASTER_JSON)

    # 2. Load ISIN map to get FnoFlag
    fno_symbols = set()
    fno_security_ids = set()
    security_id_by_symbol = {}
    if os.path.exists(MASTER_ISIN):
        for item in load_json(MASTER_ISIN):
            symbol = item.get("Symbol")
            security_id = normalized_security_id(item.get("Sid"))
            if symbol and security_id:
                security_id_by_symbol[normalized_symbol(symbol)] = security_id
            if item.get("FnoFlag") == 1 or item.get("FnoFlag") == "1":
                fno_symbols.add(symbol)
                if security_id:
                    fno_security_ids.add(security_id)

    print(f"Found {len(fno_symbols)} F&O eligible stocks from ISIN map.")

    # 3. Fetch lot sizes and expiry
    print("Fetching Dhan buildId...")
    build_id = get_build_id(BUILD_ID_PAGE)
    print(f"  BuildId: {build_id}")

    print("Fetching F&O lot sizes...")
    lot_map = fetch_lot_sizes(build_id)
    print(f"  Got lot sizes for {len(lot_map)} instruments.")

    print("Fetching F&O expiry calendar...")
    expiry_by_symbol, expiry_by_security_id = fetch_next_expiry(build_id)
    print(f"  Got expiry dates for {len(expiry_by_symbol)} symbols and {len(expiry_by_security_id)} security IDs.")

    # 4. Enrich master JSON
    enriched = 0
    expiry_matched = 0
    for stock in master_data:
        sym = stock.get("Symbol")
        # The base analysis deliberately has a compact schema and does not
        # retain Sid until standardization. Resolve it from the authoritative
        # master map before matching Dhan's underlyingSecID.
        security_id = (
            stock.get("Sid")
            or stock.get("Security ID")
            or security_id_by_symbol.get(normalized_symbol(sym))
        )

        if sym in fno_symbols or (
            normalized_security_id(security_id) is not None
            and normalized_security_id(security_id) in fno_security_ids
        ):
            stock["F&O"] = "Yes"
            stock["Lot Size"] = lot_map.get(normalized_symbol(sym), "N/A")
            expiry = lookup_expiry(expiry_by_symbol, expiry_by_security_id, sym, security_id)
            stock["Next Expiry"] = expiry or "N/A"
            expiry_matched += bool(expiry)
            enriched += 1
        else:
            stock["F&O"] = "No"
            stock["Lot Size"] = "N/A"
            stock["Next Expiry"] = "N/A"

    print(f"Successfully enriched {enriched} F&O stocks; expiry matched {expiry_matched}/{enriched}.")
    if enriched and expiry_matched == 0:
        print("F&O expiry source did not match any eligible security; refusing to publish empty expiry coverage.")
        return False
    # 5. Save only a complete enrichment result.
    save_json(MASTER_JSON, master_data, ensure_ascii=False)
    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
