import json
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_utils import BASE_DIR, get_headers

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "master_isin_map.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "company_filings")
MAX_THREADS = 20  # Fast with 20 threads
FORCE_UPDATE = True # Set to True to refresh all filings

def fetch_filings(item):
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    
    if not symbol or not isin:
        return None

    output_path = os.path.join(OUTPUT_DIR, f"{symbol}_filings.json")
    
    # Check FORCE_UPDATE flag
    if os.path.exists(output_path) and os.path.getsize(output_path) > 10 and not FORCE_UPDATE:
        return "skipped"

    # --- 1. Fetch from Old Endpoint (/company_filings) ---
    url1 = "https://ow-static-scanx.dhan.co/staticscanx/company_filings"
    data1 = []
    
    headers = get_headers()

    try:
        payload1 = {
            "data": {
                "isin": isin,
                "pg_no": 1,
                "count": 100
            }
        }
        res1 = requests.post(url1, json=payload1, headers=headers, timeout=10)
        if res1.status_code == 200:
            data1 = res1.json().get("data", []) or []
    except:
        pass

    # --- 2. Fetch from New Endpoint (/lodr) ---
    url2 = "https://ow-static-scanx.dhan.co/staticscanx/lodr"
    data2 = []
    try:
         payload2 = {
            "data": {
                "isin": isin,
                "pg_no": 1,
                "count": 100
            }
        }
         res2 = requests.post(url2, json=payload2, headers=headers, timeout=10)
         if res2.status_code == 200:
             data2 = res2.json().get("data", []) or []
    except:
        pass

    # --- 3. Merge & Deduplicate ---
    combined = data1 + data2
    unique_map = {}
    
    # We use (date + caption) or unique 'news_id' if available to deduplicate
    for entry in combined:
        nid = entry.get("news_id")
        date_str = entry.get("news_date")
        caption = entry.get("caption") or entry.get("descriptor") or "Unknown"
        
        # Create a unique key
        key = nid if nid else f"{date_str}_{caption}"
        
        # If duplicate, keep one (prefer one with file_url if possible, usually both have it)
        if key not in unique_map:
            unique_map[key] = entry
        else:
            # If current has a URL but stored doesn't, swap (rare case)
            if entry.get("file_url") and not unique_map[key].get("file_url"):
                unique_map[key] = entry

    final_list = list(unique_map.values())
    
    # Sort by date descending (latest first)
    try:
        final_list.sort(key=lambda x: x.get("news_date", "1900-01-01"), reverse=True)
    except:
        pass # Fallback if dates are weird

    if not final_list:
        return "empty"

    wrapped_data = {"code": 0, "data": final_list}
    
    with open(output_path, "w") as f:
        json.dump(wrapped_data, f, indent=4)
        
    return "success"

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print(f"Loading ISIN mapping from {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, "r") as f:
            stock_list = json.load(f)
    except Exception as e:
        print(f"Error: Could not load {INPUT_FILE}: {e}")
        return

    total = len(stock_list)
    print(f"Starting Multi-threaded Filing Fetch (Threads: {MAX_THREADS}) for {total} stocks...")
    
    success_count = 0
    skipped_count = 0
    error_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_stock = {executor.submit(fetch_filings, item): item["Symbol"] for item in stock_list}
        
        count = 0
        for future in as_completed(future_to_stock):
            count += 1
            res = future.result()
            if res == "success": success_count += 1
            elif res == "skipped": skipped_count += 1
            else: error_count += 1

            if count % 100 == 0 or count == total:
                elapsed = time.time() - start_time
                print(f"[{count}/{total}] | Success: {success_count} | Skipped: {skipped_count} | Errors: {error_count} | Elapsed: {elapsed:.1f}s")

    print("\n--- Final Report ---")
    print(f"Total Time: {time.time() - start_time:.1f}s")
    print(f"Finished: {success_count} | Errors: {error_count} | Skipped: {skipped_count}")

if __name__ == "__main__":
    main()
