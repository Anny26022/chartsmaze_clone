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

def get_last_date(csv_path):
    """Get the last date string (YYYY-MM-DD) from CSV."""
    try:
        with open(csv_path, "r") as f:
            rows = list(csv.DictReader(f))
            return rows[-1]["Date"] if rows else None
    except:
        return None

def fetch_single_stock(sym, details, start_ts, end_ts):
    output_path = os.path.join(OUTPUT_DIR, f"{sym}.csv")
    last_date = get_last_date(output_path)
    
    # If file exists and is from today/yesterday, skip
    if last_date:
        last_dt = datetime.strptime(last_date, "%Y-%m-%d")
        if (datetime.now() - last_dt).days < 1:
            return "uptodate"
        # Update start_ts to day after last_date
        start_ts = int(last_dt.timestamp()) + 86400

    api_url = "https://openweb-ticks.dhan.co/getDataH"
    payload = {
        "EXCH": details["Exch"], "SYM": sym, "SEG": details["Seg"],
        "INST": details["Inst"], "SEC_ID": details["Sid"],
        "EXPCODE": 0, "INTERVAL": "D", "START": start_ts, "END": end_ts
    }

    try:
        response = requests.post(api_url, json=payload, headers=get_headers(), timeout=15)
        if response.status_code == 200:
            data = response.json().get("data", {})
            times = data.get("Time", [])
            if not times: return "uptodate"

            # Parse new data
            new_rows = []
            o, h, l, c, v = data.get("o", []), data.get("h", []), data.get("l", []), data.get("c", []), data.get("v", [])
            for i in range(len(times)):
                dt_str = datetime.fromtimestamp(times[i]).strftime("%Y-%m-%d")
                new_rows.append({'Date': dt_str, 'Open': o[i], 'High': h[i], 'Low': l[i], 'Close': c[i], 'Volume': v[i]})

            # Append to existing
            file_exists = os.path.exists(output_path)
            with open(output_path, "a" if file_exists else "w", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                if not file_exists: writer.writeheader()
                writer.writerows(new_rows)
            
            return "success"
        return "error"
    except:
        return "error"

def main():
    if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
    with open(INPUT_FILE, "r") as f: dhan_data = json.load(f)

    stocks = {item["Sym"]: {"Sid": item["Sid"], "Exch": item.get("Exch", "NSE"), "Inst": "EQUITY", "Seg": "E"} 
              for item in dhan_data if item.get("Sym") and item.get("Sid")}

    total = len(stocks)
    start_ts, end_ts = 215634600, int(time.time())
    
    print(f"Syncing OHLCV for {total} stocks...")
    counts = {"success": 0, "uptodate": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(fetch_single_stock, s, stocks[s], start_ts, end_ts): s for s in stocks}
        for future in as_completed(futures):
            res = future.result()
            counts[res if res in counts else "error"] += 1

    print(f"Done! New: {counts['success']} | UpToDate: {counts['uptodate']} | Errors: {counts['error']}")

if __name__ == "__main__":
    main()
