import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline_utils import load_json, post_json, save_json

# --- Configuration ---
INPUT_FILE = "master_isin_map.json"
OUTPUT_FILE = "advanced_indicator_data.json"
API_URL = "https://ow-static-scanx.dhan.co/staticscanx/indicator"
MAX_THREADS = 50  # Fast parallel execution

def fetch_indicators(item):
    """Fetch advanced indicators (Pivot, Moving Averages, RSI Sentiment) for a stock."""
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    sid = item.get("Sid")
    
    # We require Sid for this API
    if not sid:
        return None

    payload = {
        "exchange": "NSE",
        "segment": "E",
        "security_id": str(sid),
        "isin": isin,
        "symbol": symbol,
        "minute": "D"  # Daily timeframe
    }

    try:
        data = post_json(API_URL, payload, timeout=10).get("data", [])
        if data and isinstance(data, list):
            result = data[0]
            return {
                "Symbol": symbol,
                "EMA": result.get("EMA", []),
                "SMA": result.get("SMA", []),
                "TechnicalIndicators": result.get("Indicator", []),
                "Pivots": result.get("Pivot", []),
            }
        return None
    except Exception:
        return None

def main():
    try:
        master_list = load_json(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found. Please run fetch_dhan_data.py first.")
        return False

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

    save_json(OUTPUT_FILE, all_results)
    print(f"Successfully saved indicators for {len(all_results)} stocks to {OUTPUT_FILE}")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
