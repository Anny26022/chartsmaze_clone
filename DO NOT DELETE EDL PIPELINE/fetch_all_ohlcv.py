import json
import requests
import os
import time
import csv
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
INPUT_FILE = "/Users/aniketmahato/Desktop/Chartsmaze/DO NOT DELETE EDL PIPELINE/dhan_data_response.json"
OUTPUT_DIR = "/Users/aniketmahato/Desktop/Chartsmaze/DO NOT DELETE EDL PIPELINE/ohlcv_data"
MAX_THREADS = 15  # Adjust based on your internet speed (15-20 is safe)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0"
]

def fetch_single_stock(sym, details, start_ts, end_ts):
    """Function to fetch a single stock's data - to be run in a thread"""
    output_path = os.path.join(OUTPUT_DIR, f"{sym}.csv")
    
    # Skip if file already exists with data
    if os.path.exists(output_path) and os.path.getsize(output_path) > 100:
        return "skipped"

    api_url = "https://openweb-ticks.dhan.co/getDataH"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS)
    }

    payload = {
        "EXCH": details["Exch"],
        "SYM": sym,
        "SEG": details["Seg"],
        "INST": details["Inst"],
        "SEC_ID": details["Sid"],
        "EXPCODE": 0,
        "INTERVAL": "D",
        "START": start_ts,
        "END": end_ts
    }

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=15)
        if response.status_code == 200:
            resp_json = response.json()
            if resp_json.get("success") and "data" in resp_json:
                data = resp_json["data"]
                times = data.get("Time", [])
                if times:
                    with open(output_path, "w", newline='') as csvfile:
                        writer = csv.DictWriter(csvfile, fieldnames=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                        writer.writeheader()
                        # Extract data lists
                        o, h, l, c, v = data.get("o", []), data.get("h", []), data.get("l", []), data.get("c", []), data.get("v", [])
                        for i in range(len(times)):
                            writer.writerow({
                                'Date': times[i], 'Open': o[i], 'High': h[i], 
                                'Low': l[i], 'Close': c[i], 'Volume': v[i]
                            })
                    return "success"
                else:
                    return "no_data"
            return "api_error"
        return f"http_{response.status_code}"
    except Exception:
        return "exception"

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print("Loading symbol mapping...")
    with open(INPUT_FILE, "r") as f:
        dhan_data = json.load(f)

    stocks_to_fetch = {}
    for item in dhan_data:
        sym, sid = item.get("Sym"), item.get("Sid")
        if sym and sid:
            stocks_to_fetch[sym] = {
                "Sid": sid, "Exch": item.get("Exch", "NSE"),
                "Inst": item.get("Inst", "EQUITY"), "Seg": item.get("Seg", "E")
            }

    symbols = list(stocks_to_fetch.keys())
    total = len(symbols)
    start_ts, end_ts = 215634600, int(time.time())

    print(f"Starting Multi-threaded Fetch (Threads: {MAX_THREADS}) for {total} stocks...")
    
    success_count = 0
    skipped_count = 0
    error_count = 0
    
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_sym = {executor.submit(fetch_single_stock, sym, stocks_to_fetch[sym], start_ts, end_ts): sym for sym in symbols}
        
        count = 0
        for future in as_completed(future_to_sym):
            count += 1
            result = future.result()
            
            if result == "success": success_count += 1
            elif result == "skipped": skipped_count += 1
            else: error_count += 1

            if count % 100 == 0 or count == total:
                elapsed = time.time() - start_time
                print(f"[{count}/{total}] | Success: {success_count} | Skipped: {skipped_count} | Errors: {error_count} | Elapsed: {elapsed:.1f}s")

    print("\n--- Final Report ---")
    print(f"Total Time: {time.time() - start_time:.1f}s")
    print(f"Finished: {success_count} | Errors: {error_count} | Skipped: {skipped_count}")

if __name__ == "__main__":
    main()
