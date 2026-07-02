import requests
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from ohlcv_utils import merge_rows_by_date, read_ohlcv_csv, rows_from_tick_data, write_ohlcv_csv
from pipeline_utils import ensure_dir, fetch_scanx_data, get_headers, load_json, resolve_path

# --- Configuration ---
INPUT_FILE = "dhan_data_response.json"
OUTPUT_DIR = "ohlcv_data"
CHUNK_DAYS = 180  # Fetch in chunks to avoid API limits
MAX_THREADS = 15
TICK_API_URL = "https://openweb-ticks.dhan.co/getDataH"

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
        return {item["Sym"]: item for item in fetch_scanx_data(payload, timeout=15) if item.get("Sym")}
    except Exception:
        pass
    return {}

def fetch_history_chunk(payload):
    """Fetch a single chunk of historical data."""
    try:
        response = requests.post(TICK_API_URL, json=payload, headers=get_headers(include_origin=True), timeout=15)
        if response.status_code == 200:
            return rows_from_tick_data(response.json().get("data", {}))
    except Exception:
        pass
    return []

def fetch_single_stock(sym, details, live_snapshot=None):
    output_path = resolve_path(OUTPUT_DIR) / f"{sym}.csv"
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # 1. Determine starting point
    # Default start: 2 years ago (approx 500 trading days)
    # We use 500 days to ensure enough history for 200MA and other technicals
    global_start_ts = int(time.time()) - (2 * 365 * 86400) 
    target_start = global_start_ts
    
    existing_rows = read_ohlcv_csv(output_path)
    if existing_rows:
        try:
            last_date = existing_rows[-1]["Date"]
            last_dt = datetime.strptime(last_date, "%Y-%m-%d")
            target_start = int(last_dt.timestamp()) + 86400
        except Exception:
            pass

    # 2. Fetch missing history in chunks
    new_rows = []
    current_end = int(time.time())
    
    # Only fetch if there's a gap before today
    if target_start < current_end - 86400:
        # Fetch backwards from now to target_start in chunks
        chunk_ptr = current_end
        while chunk_ptr > target_start:
            c_start = max(target_start, chunk_ptr - (CHUNK_DAYS * 86400))
            payload = {
                "EXCH": details["Exch"], "SYM": sym, "SEG": details["Seg"],
                "INST": details["Inst"], "SEC_ID": details["Sid"],
                "EXPCODE": 0, "INTERVAL": "D", "START": int(c_start), "END": int(chunk_ptr)
            }
            chunk_rows = fetch_history_chunk(payload)
            if chunk_rows:
                new_rows.extend(chunk_rows)
            
            # If we didn't get data for this chunk, move on to avoid infinite loop
            chunk_ptr = c_start - 86400

    # 3. Hybrid Step: Add Today using Live Snapshot
    if live_snapshot:
        s = live_snapshot
        today_row = {
            'Date': today_str, 
            'Open': s.get('Open', 0), 
            'High': s.get('High', 0), 
            'Low': s.get('Low', 0), 
            'Close': s.get('Ltp', 0), 
            'Volume': s.get('Volume', 0)
        }
        new_rows.append(today_row)

    if not new_rows: 
        return "uptodate"

    # 4. Merge and Deduplicate
    final_rows = merge_rows_by_date(existing_rows + new_rows)

    if not final_rows: 
        return "uptodate"

    write_ohlcv_csv(output_path, final_rows)
    return "success"

def main():
    ensure_dir(OUTPUT_DIR)

    try:
        dhan_data = load_json(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found.")
        return False

    stocks = {item["Sym"]: {"Sid": item["Sid"], "Exch": item.get("Exch", "NSE"), "Inst": "EQUITY", "Seg": "E"} 
              for item in dhan_data if item.get("Sym") and item.get("Sid")}

    # Get live snapshots for today's data
    live_snapshots = get_live_snapshots()

    print(f"Syncing OHLCV for {len(stocks)} stocks (Hybrid Multi-Chunk Mode)...")
    counts = {"success": 0, "uptodate": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(fetch_single_stock, s, stocks[s], live_snapshots.get(s)): s for s in stocks}
        for future in as_completed(futures):
            try:
                res = future.result()
                counts[res if res in counts else "error"] += 1
            except Exception:
                counts["error"] += 1

    print(f"Done! Updated: {counts['success']} | UpToDate: {counts['uptodate']} | Errors: {counts['error']}")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
