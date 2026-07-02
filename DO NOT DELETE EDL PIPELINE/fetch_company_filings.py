import sys
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_utils import ensure_dir, get_headers, load_json, resolve_path, save_json

# --- Configuration ---
INPUT_FILE = "master_isin_map.json"
OUTPUT_DIR = "company_filings"
LEGACY_URL = "https://ow-static-scanx.dhan.co/staticscanx/company_filings"
LODR_URL = "https://ow-static-scanx.dhan.co/staticscanx/lodr"
MAX_THREADS = 20  # Fast with 20 threads
FORCE_UPDATE = True # Set to True to refresh all filings


def fetch_endpoint(url, isin, headers):
    payload = {"data": {"isin": isin, "pg_no": 1, "count": 100}}
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json().get("data", []) or []
    except Exception:
        pass
    return []


def dedupe_filings(items):
    unique_map = {}
    for entry in items:
        news_id = entry.get("news_id")
        date_str = entry.get("news_date")
        caption = entry.get("caption") or entry.get("descriptor") or "Unknown"
        key = news_id if news_id else f"{date_str}_{caption}"

        if key not in unique_map or (entry.get("file_url") and not unique_map[key].get("file_url")):
            unique_map[key] = entry

    final_list = list(unique_map.values())
    final_list.sort(key=lambda x: x.get("news_date", "1900-01-01"), reverse=True)
    return final_list

def fetch_filings(item):
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    
    if not symbol or not isin:
        return None

    output_path = resolve_path(OUTPUT_DIR) / f"{symbol}_filings.json"

    # Check FORCE_UPDATE flag
    if output_path.exists() and output_path.stat().st_size > 10 and not FORCE_UPDATE:
        return "skipped"

    headers = get_headers()
    final_list = dedupe_filings(
        fetch_endpoint(LEGACY_URL, isin, headers) + fetch_endpoint(LODR_URL, isin, headers)
    )

    if not final_list:
        return "empty"

    save_json(output_path, {"code": 0, "data": final_list})
    return "success"

def main():
    ensure_dir(OUTPUT_DIR)

    print(f"Loading ISIN mapping from {INPUT_FILE}...")
    try:
        stock_list = load_json(INPUT_FILE)
    except Exception as e:
        print(f"Error: Could not load {INPUT_FILE}: {e}")
        return False

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
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
