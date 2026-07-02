import sys

from dhan_next_utils import find_nested_list, get_build_id, get_embedded_next_data, get_next_data
from pipeline_utils import save_json


PAGE_URL = "https://dhan.co/nse-fno-lot-size/"
NEXT_PAGE = "nse-fno-lot-size"
OUTPUT_FILE = "fno_lot_sizes_cleaned.json"


def is_lot_size_list(items):
    return len(items) > 10 and isinstance(items[0], dict) and ("sym" in items[0] or "symbol" in items[0])


def load_instrument_list(build_id):
    data = get_next_data(build_id, NEXT_PAGE)
    instrument_list = data.get("pageProps", {}).get("listData", [])
    if instrument_list:
        return instrument_list

    print("Falling back to BeautifulSoup extraction...")
    embedded = get_embedded_next_data(PAGE_URL)
    return find_nested_list(embedded.get("props", {}), is_lot_size_list) or []


def clean_lot_size_item(item):
    lots = {}
    for index, contract in enumerate(item.get("fo_dt", [])):
        contract_sym = contract.get("sym", "")
        try:
            month_label = contract_sym.split("-")[1]
        except Exception:
            month_label = f"Month_{index + 1}"
        lots[f"Lot_{month_label}"] = contract.get("ls")

    entry = {"Symbol": item.get("sym") or item.get("symbol"), "Name": item.get("disp") or item.get("companyName")}
    entry.update(lots)
    return entry

def fetch_fno_lot_sizes():
    build_id = get_build_id(PAGE_URL)

    print(f"Primary Fetch: Direct JSON via Next.js Data API...")
    try:
        instrument_list = load_instrument_list(build_id)

        if not instrument_list:
            print("ERROR: Could not locate lot size data.")
            return False

        final_list = [clean_lot_size_item(item) for item in instrument_list]
        save_json(OUTPUT_FILE, final_list)
            
        print(f"Successfully extracted {len(final_list)} instruments via Direct JSON.")
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        return False

if __name__ == "__main__":
    sys.exit(0 if fetch_fno_lot_sizes() else 1)
