import json
import requests
import os
import time
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_utils import BASE_DIR, get_headers

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "dhan_data_response.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "ohlcv_data")
MAX_THREADS = 15
SCANX_URL = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"

def get_live_snapshots():
    """Fetches live OHLCV snapshot for all stocks to fill in Today's gap."""
    print("Fetching live snapshots for stocks (Today's data)...")
    payload = {
        "data": {
            "sort": "Volume", "sorder": "desc", "count": 5000,
            "fields": ["Sym", "Open", "High", "Low", "Ltp", "Volume"],
            "params": [{"field": "Exch", "op": "", "val": "NSE"}]
        }
    }
    try:
        response = requests.post(SCANX_URL, json=payload, headers=get_headers(), timeout=15)
        if response.status_code == 200:
            return {i['Sym']: i for i in response.json().get('data', [])}
    except: pass
    return {}

def get_last_date(csv_path):
    """Get the last date string (YYYY-MM-DD) from CSV."""
    try:
        with open(csv_path, "r") as f:
            rows = list(csv.DictReader(f))
            return rows[-1]["Date"] if rows else None
    except:
        return None

def fetch_single_stock(sym, details, start_ts, end_ts, live_snapshot=None):
    output_path = os.path.join(OUTPUT_DIR, f"{sym}.csv")
    last_date = get_last_date(output_path)
    today_str = datetime.now().strftime("%Y-%m-%d")

    # 1. Fetch Historical Chunks
    api_url = "https://openweb-ticks.dhan.co/getDataH"
    payload = {
        "EXCH": details["Exch"], "SYM": sym, "SEG": details["Seg"],
        "INST": details["Inst"], "SEC_ID": details["Sid"],
        "EXPCODE": 0, "INTERVAL": "D", "START": start_ts, "END": end_ts
    }

    new_rows = []
    try:
        response = requests.post(api_url, json=payload, headers=get_headers(), timeout=15)
        if response.status_code == 200:
            data = response.json().get("data", {})
            times = data.get("Time", [])
            if times:
                o, h, l, c, v = data.get("o", []), data.get("h", []), data.get("l", []), data.get("c", []), data.get("v", [])
                for i in range(len(times)):
                    dt_str = datetime.fromtimestamp(times[i]).strftime("%Y-%m-%d")
                    new_rows.append({'Date': dt_str, 'Open': o[i], 'High': h[i], 'Low': l[i], 'Close': c[i], 'Volume': v[i]})
    except: pass

    # 2. Hybrid Step: Add/Update Today using Live Snapshot
    if live_snapshot:
        s = live_snapshot
        # Map LTP as Close
        today_row = {
            'Date': today_str, 
            'Open': s.get('Open', 0), 
            'High': s.get('High', 0), 
            'Low': s.get('Low', 0), 
            'Close': s.get('Ltp', 0), 
            'Volume': s.get('Volume', 0)
        }
        
        # If today is already in new_rows, update it
        exists = False
        for i, row in enumerate(new_rows):
            if row['Date'] == today_str:
                new_rows[i] = today_row
                exists = True
        if not exists:
            new_rows.append(today_row)

    if not new_rows: return "uptodate"

    # 3. Read existing and merge
    existing_rows = []
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            existing_rows = list(csv.DictReader(f))

    # Deduplicate by Date (keep the latest one)
    merged = {r['Date']: r for r in existing_rows + new_rows}
    final_rows = sorted(merged.values(), key=lambda x: x['Date'])

    if not final_rows: return "uptodate"

    with open(output_path, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        writer.writeheader()
        writer.writerows(final_rows)
    
    return "success"

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    if not os.path.exists(INPUT_FILE): return

    with open(INPUT_FILE, "r") as f: dhan_data = json.load(f)

    stocks = {item["Sym"]: {"Sid": item["Sid"], "Exch": item.get("Exch", "NSE"), "Inst": "EQUITY", "Seg": "E"} 
              for item in dhan_data if item.get("Sym") and item.get("Sid")}

    # Get live snapshots for today's data
    live_snapshots = get_live_snapshots()

    total = len(stocks)
    start_ts, end_ts = 215634600, int(time.time())
    
    print(f"Syncing OHLCV for {total} stocks (Hybrid Mode)...")
    counts = {"success": 0, "uptodate": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(fetch_single_stock, s, stocks[s], start_ts, end_ts, live_snapshots.get(s)): s for s in stocks}
        for future in as_completed(futures):
            try:
                res = future.result()
                counts[res if res in counts else "error"] += 1
            except:
                counts["error"] += 1

    print(f"Done! Updated: {counts['success']} | UpToDate: {counts['uptodate']} | Errors: {counts['error']}")

if __name__ == "__main__":
    main()
