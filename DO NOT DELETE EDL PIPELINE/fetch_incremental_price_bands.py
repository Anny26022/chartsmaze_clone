import sys

from nse_archive_utils import fetch_latest_nse_csv
from pipeline_utils import save_json


BASE_URL = "https://nsearchives.nseindia.com/content/equities/eq_band_changes_{date}.csv"
OUTPUT_FILE = "incremental_price_bands.json"

def fetch_nse_price_bands():
    clean_data = fetch_latest_nse_csv(BASE_URL, "price band changes", timeout=10)

    if clean_data:
        save_json(OUTPUT_FILE, clean_data)
        print(f"Successfully saved {len(clean_data)} price band changes to {OUTPUT_FILE}")
        return True

    print("Could not find any price band files in the last 7 days.")
    return False

if __name__ == "__main__":
    sys.exit(0 if fetch_nse_price_bands() else 1)
