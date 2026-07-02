import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipeline_utils import load_json, post_json, save_json

# --- Configuration ---
INPUT_FILE = "master_isin_map.json"
OUTPUT_FILE = "all_company_announcements.json"
API_URL = "https://ow-static-scanx.dhan.co/staticscanx/announcements"
MAX_THREADS = 40  # Faster for small payloads

def fetch_announcements(item):
    """Fetch announcements for a single ISIN"""
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    name = item.get("Name")

    payload = {"data": {"isin": isin}}

    try:
        announcements = post_json(API_URL, payload, timeout=10).get("data")
        if announcements and isinstance(announcements, list):
            return [
                {
                    "Symbol": symbol,
                    "Name": name,
                    "Event": ann.get("events"),
                    "Date": ann.get("date"),
                    "Type": ann.get("type")
                } for ann in announcements
            ]
        return None
    except Exception:
        return None

def main():
    try:
        master_list = load_json(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found.")
        return False

    print(f"Starting fetch for {len(master_list)} stocks...")
    all_results = []
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_stock = {executor.submit(fetch_announcements, stock): stock for stock in master_list}
        
        completed = 0
        for future in as_completed(future_to_stock):
            res = future.result()
            if res:
                all_results.extend(res)
            completed += 1
            if completed % 100 == 0:
                print(f"Progress: {completed}/{len(master_list)} done.")

    # Sort results by date descending
    all_results.sort(key=lambda x: x.get("Date", ""), reverse=True)

    save_json(OUTPUT_FILE, all_results)
    print(f"Successfully saved {len(all_results)} announcements to {OUTPUT_FILE}")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
