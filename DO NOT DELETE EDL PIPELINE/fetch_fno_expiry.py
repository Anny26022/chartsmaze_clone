import sys

from dhan_next_utils import get_build_id, get_embedded_next_data, get_next_data
from pipeline_utils import save_json


BUILD_ID_PAGE = "https://dhan.co/all-indices/"
PAGE_URL = "https://dhan.co/fno-expiry-calendar/"
NEXT_PAGE = "fno-expiry-calendar"
OUTPUT_FILE = "fno_expiry_calendar.json"


def load_expiry_data(build_id):
    data = get_next_data(build_id, NEXT_PAGE)
    expiry_data = data.get("pageProps", {}).get("expiryData", {}).get("data", [])
    if expiry_data:
        return expiry_data

    print("Falling back to BeautifulSoup extraction...")
    embedded = get_embedded_next_data(PAGE_URL)
    return embedded.get("props", {}).get("pageProps", {}).get("expiryData", {}).get("data", [])


def flatten_expiry_data(expiry_data_raw):
    flattened_data = []
    for exchange_data in expiry_data_raw:
        exchange = exchange_data.get("exch")
        segment = exchange_data.get("seg")
        for exp_group in exchange_data.get("exps", []):
            inst_type = exp_group.get("inst")
            for item in exp_group.get("explst", []):
                flattened_data.append({
                    "Exchange": exchange,
                    "Segment": segment,
                    "InstrumentType": inst_type,
                    "SymbolName": item.get("symbolName"),
                    "ExpiryDate": item.get("expdate"),
                    "UnderlyingSecID": item.get("underlyingSecID"),
                })
    return flattened_data

def fetch_fno_expiry_calendar():
    build_id = get_build_id(BUILD_ID_PAGE)
    if not build_id:
        print("Could not find dynamic buildId. Falling back to static extraction...")

    print(f"Primary Fetch: Direct JSON via Next.js Data API...")
    try:
        expiry_data_raw = load_expiry_data(build_id)
        if not expiry_data_raw:
            print("ERROR: Could not locate expiry data from any source.")
            return False

        flattened_data = flatten_expiry_data(expiry_data_raw)
        save_json(OUTPUT_FILE, flattened_data)
        
        print(f"Successfully processed {len(flattened_data)} expiry entries.")
        print(f"Data saved to {OUTPUT_FILE}")
        return True

    except Exception as e:
        print(f"FAILED to fetch expiry calendar: {e}")
        return False

if __name__ == "__main__":
    sys.exit(0 if fetch_fno_expiry_calendar() else 1)
