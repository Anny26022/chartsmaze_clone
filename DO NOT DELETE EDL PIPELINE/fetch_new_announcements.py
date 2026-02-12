import json
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_utils import BASE_DIR, get_headers

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "master_isin_map.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "all_company_announcements.json")
MAX_THREADS = 40  # Faster for small payloads

def fetch_announcements(item):
    """Fetch announcements for a single ISIN"""
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    name = item.get("Name")
    
    api_url = "https://ow-static-scanx.dhan.co/staticscanx/announcements"
    headers = get_headers()

    payload = {"data": {"isin": isin}}

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            res_json = response.json()
            announcments = res_json.get("data")
            if announcments and isinstance(announcments, list):
                # Return list of mapped results
                return [
                    {
                        "Symbol": symbol,
                        "Name": name,
                        "Event": ann.get("events"),
                        "Date": ann.get("date"),
                        "Type": ann.get("type")
                    } for ann in announcments
                ]
        return None
    except Exception as e:
        return None

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, "r") as f:
        master_list = json.load(f)

    print(f"Starting fetch for {len(master_list)} stocks...")
    all_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_stock = {executor.submit(fetch_announcements, stock): stock for stock in master_list}
        
        completed = 0
        for future in as_completed(future_to_stock):
            res = future.result()
            if res:
                all_results.extend(res)
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed}/{len(master_list)} done.")

    # Sort results by date descending
    all_results.sort(key=lambda x: x.get("Date", ""), reverse=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_results, f, indent=4)

    print(f"Successfully saved {len(all_results)} announcements to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
