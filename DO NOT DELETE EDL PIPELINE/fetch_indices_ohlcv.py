"""
ULTRA-FAST Index OHLCV Fetcher - Hybrid Incremental
Merges deep history with Today's live snapshot from ScanX API.
"""

import json
import requests
import os
import time
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_utils import BASE_DIR, get_headers

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "all_indices_list.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "indices_ohlcv_data")
CHUNK_DAYS = 120
MAX_THREADS = 60

def get_safe_sym(sym):
    return "".join([c if c.isalnum() else "_" for c in sym])

def fetch_chunk(payload):
    try:
        r = requests.post("https://openweb-ticks.dhan.co/getDataH", json=payload, headers=get_headers(), timeout=10)
        if r.status_code == 200:
            data = r.json().get("data", {})
            times = data.get("Time", [])
            if not times: return []
            
            rows = []
            o, h, l, c, v = data.get("o", []), data.get("h", []), data.get("l", []), data.get("c", []), data.get("v", [])
            for i in range(len(times)):
                t = times[i]
                dt_str = t if isinstance(t, str) else datetime.fromtimestamp(t).strftime("%Y-%m-%d")
                rows.append({'Date': dt_str, 'Open': o[i], 'High': h[i], 'Low': l[i], 'Close': c[i], 'Volume': v[i]})
            return rows
    except:
        pass
    return []

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    if not os.path.exists(INPUT_FILE): return

    with open(INPUT_FILE, "r") as f: indices = json.load(f)

    tasks = []
    global_start_ts = 215634600 # 1976
    global_end_ts = int(time.time())
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    existing_data_cache = {}
    print(f"Checking {len(indices)} indices for sync...")

    for idx in indices:
        sym = idx["Symbol"]
        safe_sym = get_safe_sym(sym)
        output_path = os.path.join(OUTPUT_DIR, f"{safe_sym}.csv")
        
        target_start = global_start_ts
        if os.path.exists(output_path):
            try:
                with open(output_path, "r") as f:
                    rows = list(csv.DictReader(f))
                    if rows:
                        existing_data_cache[safe_sym] = rows
                        last_row_date = rows[-1]["Date"]
                        # We still schedule a chunk for recent days to ensure gap-filling
                        last_dt = datetime.strptime(last_row_date, "%Y-%m-%d")
                        target_start = int(last_dt.timestamp()) + 86400
            except: pass

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
        
        # Deduplicate and update
        merged = {r['Date']: r for r in all_rows}
        merged[today_str] = today_row
        
        final_rows = sorted(merged.values(), key=lambda x: x['Date'])
        
        output_path = os.path.join(OUTPUT_DIR, f"{safe_sym}.csv")
        with open(output_path, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
            writer.writeheader()
            writer.writerows(final_rows)

    print(f"Successfully updated all index CSVs with Today's Live data.")

if __name__ == "__main__":
    main()
