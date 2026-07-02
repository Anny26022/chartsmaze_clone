"""
ULTRA-FAST Index OHLCV Fetcher - Hybrid Incremental
Merges deep history with Today's live snapshot from ScanX API.
"""

import requests
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from ohlcv_utils import merge_rows_by_date, read_ohlcv_csv, rows_from_tick_data, write_ohlcv_csv
from pipeline_utils import ensure_dir, get_headers, load_json, resolve_path

# --- Configuration ---
INPUT_FILE = "all_indices_list.json"
OUTPUT_DIR = "indices_ohlcv_data"
TICK_API_URL = "https://openweb-ticks.dhan.co/getDataH"
CHUNK_DAYS = 120
MAX_THREADS = 60

def get_safe_sym(sym):
    return "".join([c if c.isalnum() else "_" for c in sym])

def fetch_chunk(payload):
    try:
        r = requests.post(TICK_API_URL, json=payload, headers=get_headers(), timeout=10)
        if r.status_code == 200:
            return rows_from_tick_data(r.json().get("data", {}))
    except Exception:
        pass
    return []

def main():
    ensure_dir(OUTPUT_DIR)

    try:
        indices = load_json(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found.")
        return False

    tasks = []
    global_start_ts = 215634600 # 1976
    global_end_ts = int(time.time())
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    existing_data_cache = {}
    print(f"Checking {len(indices)} indices for sync...")

    for idx in indices:
        sym = idx["Symbol"]
        safe_sym = get_safe_sym(sym)
        output_path = resolve_path(OUTPUT_DIR) / f"{safe_sym}.csv"
        
        target_start = global_start_ts
        rows = read_ohlcv_csv(output_path)
        if rows:
            try:
                existing_data_cache[safe_sym] = rows
                last_row_date = rows[-1]["Date"]
                last_dt = datetime.strptime(last_row_date, "%Y-%m-%d")
                target_start = int(last_dt.timestamp()) + 86400
            except Exception:
                pass

        # Only crawl if there's a gap before today
        if target_start < global_end_ts - 86400:
            current_end = global_end_ts
            while current_end > target_start:
                c_start = max(target_start, current_end - (CHUNK_DAYS * 86400))
                tasks.append({
                    "EXCH": idx["Exchange"], "SYM": sym, "SEG": idx["Segment"],
                    "INST": idx["Instrument"], "SEC_ID": idx["IndexID"],
                    "EXPCODE": 0, "INTERVAL": "D", "START": c_start, "END": current_end,
                    "SAFE_SYM": safe_sym
                })
                current_end = c_start - 86400

    # Execute history crawl if needed
    new_data = {get_safe_sym(i["Symbol"]): [] for i in indices}
    if tasks:
        print(f"Executing {len(tasks)} API chunks for history...")
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_payload = {executor.submit(fetch_chunk, t): t for t in tasks}
            for future in as_completed(future_to_payload):
                payload = future_to_payload[future]
                rows = future.result()
                if rows:
                    new_data[payload["SAFE_SYM"]].extend(rows)

    print("Merging with Live Snapshots and saving CSVs...")
    for idx in indices:
        safe_sym = get_safe_sym(idx["Symbol"])
        
        # 1. Start with existing or historic data
        base_rows = existing_data_cache.get(safe_sym, [])
        fetched_rows = new_data.get(safe_sym, [])
        all_rows = base_rows + fetched_rows
        
        # 2. Add TODAY'S snapshot from all_indices_list.json
        # Ltp is Close for the running day
        today_row = {
            'Date': today_str, 
            'Open': idx.get('Open'), 
            'High': idx.get('High'), 
            'Low': idx.get('Low'), 
            'Close': idx.get('Ltp'), 
            'Volume': idx.get('Volume', 0)
        }
        
        final_rows = merge_rows_by_date(all_rows + [today_row])
        output_path = resolve_path(OUTPUT_DIR) / f"{safe_sym}.csv"
        write_ohlcv_csv(output_path, final_rows)

    print(f"Successfully updated all index CSVs with Today's Live data.")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
