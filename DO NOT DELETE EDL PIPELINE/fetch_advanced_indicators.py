import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_utils import BASE_DIR, get_headers

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "master_isin_map.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "advanced_indicator_data.json")
MAX_THREADS = 50  # Fast parallel execution

def fetch_indicators(item):
    """Fetch advanced indicators (Pivot, Moving Averages, RSI Sentiment) for a stock."""
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    sid = item.get("Sid")
    
    # We require Sid for this API
    if not sid:
        return None

    api_url = "https://ow-static-scanx.dhan.co/staticscanx/indicator"
    headers = get_headers()
    
    payload = {
        "exchange": "NSE",
        "segment": "E",
        "security_id": str(sid),
        "isin": isin,
        "symbol": symbol,
        "minute": "D"  # Daily timeframe
    }

    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data and isinstance(data, list) and len(data) > 0:
                result = data[0]
                
                # Extract specific fields we care about
                ema_data = result.get("EMA", [])
                sma_data = result.get("SMA", [])
                indicators = result.get("Indicator", [])
                pivots = result.get("Pivot", [])
                
                return {
                    "Symbol": symbol,
                    "EMA": ema_data,
                    "SMA": sma_data,
                    "TechnicalIndicators": indicators,
                    "Pivots": pivots
                }
        return None
    except:
        return None

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Please run fetch_dhan_data.py first.")
        return

    with open(INPUT_FILE, "r") as f:
        master_list = json.load(f)

    print(f"Starting advanced indicator fetch for {len(master_list)} stocks...")
    all_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_stock = {executor.submit(fetch_indicators, stock): stock for stock in master_list}
        
        completed = 0
        for future in as_completed(future_to_stock):
            res = future.result()
            if res:
                all_results.append(res)
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed}/{len(master_list)} done.")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_results, f, indent=4)

    print(f"Successfully saved indicators for {len(all_results)} stocks to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
