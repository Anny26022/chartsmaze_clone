import sys

from nse_archive_utils import fetch_latest_nse_csv
from pipeline_utils import save_json


BASE_URL = "https://nsearchives.nseindia.com/content/equities/sec_list_{date}.csv"
OUTPUT_FILE = "complete_price_bands.json"

def fetch_nse_security_list():
    clean_data = fetch_latest_nse_csv(BASE_URL, "security list", timeout=15)

    if clean_data:
        save_json(OUTPUT_FILE, clean_data)
        print(f"Successfully saved {len(clean_data)} securities to {OUTPUT_FILE}")
        return True

    print("Could not find any security list files in the last 7 days.")
    return False

if __name__ == "__main__":
    sys.exit(0 if fetch_nse_security_list() else 1)
