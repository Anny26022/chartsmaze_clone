import json
import requests
import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
INPUT_FILE = "/Users/aniketmahato/Desktop/Chartsmaze/DO NOT DELETE EDL PIPELINE/master_isin_map.json"
OUTPUT_DIR = "/Users/aniketmahato/Desktop/Chartsmaze/DO NOT DELETE EDL PIPELINE/company_filings"
MAX_THREADS = 20  # Fast with 20 threads

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:121.0) Gecko/20100101 Firefox/121.0"
]

def fetch_filings(item):
    """Fetch filings for a single ISIN"""
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    output_path = os.path.join(OUTPUT_DIR, f"{symbol}_filings.json")
    
    if os.path.exists(output_path) and os.path.getsize(output_path) > 10:
        return "skipped"

    api_url = "https://ow-static-scanx.dhan.co/staticscanx/company_filings"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS)
    }

    # count: 100 as per user request
    payload = {"data": {"isin": isin, "count": 100}}

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if data:
                with open(output_path, "w") as f:
                    json.dump(data, f, indent=4)
                return "success"
            return "no_data"
        return f"http_{response.status_code}"
    except Exception:
        return "exception"

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
