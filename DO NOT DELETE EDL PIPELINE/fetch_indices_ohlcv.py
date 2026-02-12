"""
ULTRA-FAST Index OHLCV Fetcher
Bypasses bar limits by parallelizing 120-day CHUNKS across all indices.
Reduces first-run time by 10x.
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
MAX_THREADS = 60  # Aggressive parallel threads

def get_safe_sym(sym):
    return "".join([c if c.isalnum() else "_" for c in sym])

def fetch_chunk(payload):
    """Worker to fetch a single OHLCV chunk."""
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

    # 1. Prepare all tasks (all chunks for all indices)
    tasks = []
    global_start_ts = 215634600  # 1976
    global_end_ts = int(time.time())
    
    print(f"Preparing chunks for {len(indices)} indices...")
    for idx in indices:
        sym = idx["Symbol"]
        current_end = global_end_ts
        while current_end > global_start_ts:
            c_start = max(global_start_ts, current_end - (CHUNK_DAYS * 86400))
            payload = {
                "EXCH": idx["Exchange"], "SYM": sym, "SEG": idx["Segment"],
                "INST": idx["Instrument"], "SEC_ID": idx["IndexID"],
                "EXPCODE": 0, "INTERVAL": "D", "START": c_start, "END": current_end,
                "SAFE_SYM": get_safe_sym(sym) # Helper tag
            }
            tasks.append(payload)
            current_end = c_start - 86400

    # 2. Execute with massive thread pool
    print(f"Executing {len(tasks)} API requests across {MAX_THREADS} threads...")
    all_data = {get_safe_sym(i["Symbol"]): [] for i in indices}
    
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_payload = {executor.submit(fetch_chunk, t): t for t in tasks}
        
        count = 0
        for future in as_completed(future_to_payload):
            payload = future_to_payload[future]
            rows = future.result()
            if rows:
                all_data[payload["SAFE_SYM"]].extend(rows)
            
            count += 1
            if count % 200 == 0 or count == len(tasks):
                print(f"Progress: {count}/{len(tasks)} chunks fetched...")

    # 3. Assemble and Save CSVs
    print("Consolidating and saving CSVs...")
    saved_count = 0
    for safe_sym, rows in all_data.items():
        if not rows: continue
        
        # Unique & Sorted
        final_rows = sorted({r["Date"]: r for r in rows}.values(), key=lambda x: x["Date"])
        
        output_path = os.path.join(OUTPUT_DIR, f"{safe_sym}.csv")
        with open(output_path, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
            writer.writeheader()
            writer.writerows(final_rows)
        saved_count += 1

    print(f"\nDone! Processed {len(tasks)} chunks in {time.time() - start_time:.1f}s.")
    print(f"Saved {saved_count} Index CSVs to {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
