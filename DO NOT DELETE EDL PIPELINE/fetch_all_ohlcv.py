import requests
import os
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from ohlcv_utils import (
    chunk_history_range,
    merge_rows_by_date,
    plan_history_ranges,
    read_ohlcv_csv,
    rows_from_tick_data,
    symbol_csv_path,
    write_ohlcv_csv,
)
from pipeline_utils import ensure_dir, fetch_scanx_data, get_headers, load_json, resolve_path

# --- Configuration ---
INPUT_FILE = "dhan_data_response.json"
OUTPUT_DIR = "ohlcv_data"
CHUNK_DAYS = 180  # Fetch in chunks to avoid API limits
MAX_THREADS = 15
TICK_API_URL = "https://openweb-ticks.dhan.co/getDataH"
HISTORY_CALENDAR_DAYS = int(os.getenv("EDL_OHLCV_HISTORY_DAYS", str(4 * 365)))
FETCH_ATTEMPTS = 3

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
    last_error = None
    for attempt in range(FETCH_ATTEMPTS):
        try:
            response = requests.post(
                TICK_API_URL,
                json=payload,
                headers=get_headers(include_origin=True),
                timeout=15,
            )
            response.raise_for_status()
            return rows_from_tick_data(response.json().get("data", {}))
        except (requests.RequestException, ValueError, TypeError, IndexError) as error:
            last_error = error
            if attempt + 1 < FETCH_ATTEMPTS:
                time.sleep(0.25 * (2 ** attempt))
    raise RuntimeError("Historical OHLCV chunk failed after retries") from last_error

def fetch_single_stock(sym, details, live_snapshot=None):
    output_path = symbol_csv_path(resolve_path(OUTPUT_DIR), sym)
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Four calendar years gives roughly 1,000 trading sessions. This supports
    # a 250-session published window, a prior 252-session high/low reference,
    # and a stable EMA-200 warm-up.
    current_end = int(time.time())
    desired_start = current_end - (HISTORY_CALENDAR_DAYS * 86400)
    existing_rows = read_ohlcv_csv(output_path)

    # 1. Fetch both an older backfill gap and a newer incremental gap.
    new_rows = []
    for range_start, range_end in plan_history_ranges(existing_rows, desired_start, current_end):
        for c_start, c_end in chunk_history_range(range_start, range_end, CHUNK_DAYS):
            payload = {
                "EXCH": details["Exch"], "SYM": sym, "SEG": details["Seg"],
                "INST": details["Inst"], "SEC_ID": details["Sid"],
                "EXPCODE": 0, "INTERVAL": "D", "START": int(c_start), "END": int(c_end)
            }
            chunk_rows = fetch_history_chunk(payload)
            if chunk_rows:
                new_rows.extend(chunk_rows)

    # 2. Hybrid Step: Add Today using Live Snapshot
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

    # 3. Merge and Deduplicate
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
    return counts["error"] == 0

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
