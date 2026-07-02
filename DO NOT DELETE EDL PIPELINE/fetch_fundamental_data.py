import sys
import time

import requests

from pipeline_utils import chunked, get_headers, load_json, save_json


MASTER_MAP_FILE = "master_isin_map.json"
API_URL = "https://open-web-scanx.dhan.co/scanx/fundamental"
OUTPUT_FILE = "fundamental_data.json"
BATCH_SIZE = 100
REQUEST_DELAY_SECONDS = 0.5


def build_isin_lookup(master_map):
    return {
        item["ISIN"]: {"Symbol": item.get("Symbol"), "Name": item.get("Name")}
        for item in master_map
        if item.get("ISIN")
    }


def attach_symbol_metadata(rows, isin_lookup):
    for item in rows:
        metadata = isin_lookup.get(item.get("isin"))
        if metadata:
            item.update(metadata)
    return rows

def fetch_fundamental_data():
    headers = get_headers()

    # 1. Load ISINs from Master Map
    try:
        master_map = load_json(MASTER_MAP_FILE)
    except FileNotFoundError:
        print(f"Error: {MASTER_MAP_FILE} not found. Please run 'fetch_dhan_data.py' first.")
        return False

    isin_lookup = build_isin_lookup(master_map)
    all_isins = list(isin_lookup.keys())
    total_isins = len(all_isins)
    print(f"Loaded {total_isins} ISINs from master map.")

    all_fundamental_data = []
    
    for start_index, batch_isins in chunked(all_isins, BATCH_SIZE):
        batch_number = start_index // BATCH_SIZE + 1
        end_index = start_index + len(batch_isins)
        print(f"Fetching batch {batch_number}: {len(batch_isins)} ISINs ({start_index}-{end_index})...")
        payload = {"data": {"isins": batch_isins}}
        
        try:
            response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success':
                    batch_results = data.get('data', [])
                    if batch_results:
                        all_fundamental_data.extend(attach_symbol_metadata(batch_results, isin_lookup))
                        print(f"  Success: Received {len(batch_results)} records.")
                    else:
                        print("  Warning: No data returned for this batch.")
                else:
                    print(f"  API Error: {data.get('message')}")
            else:
                print(f"  HTTP Error: {response.status_code}")
                
        except Exception as e:
            print(f"  Exception fetching batch: {e}")
            
        time.sleep(REQUEST_DELAY_SECONDS)

    # 3. Save Consolidated Data
    if all_fundamental_data:
        save_json(OUTPUT_FILE, all_fundamental_data)
        print(f"\nSuccessfully saved fundamental data for {len(all_fundamental_data)} securities to {OUTPUT_FILE}")
        return True

    print("\nFailed to fetch any fundamental data.")
    return False

if __name__ == "__main__":
    sys.exit(0 if fetch_fundamental_data() else 1)
