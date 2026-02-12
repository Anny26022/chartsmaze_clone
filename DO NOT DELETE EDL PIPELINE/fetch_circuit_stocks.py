import requests
import json
import re
from bs4 import BeautifulSoup

def get_build_id():
    """Dynamically fetch the Next.js buildId."""
    url = "https://dhan.co/all-indices/"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        match = re.search(r'"buildId":"([^"]+)"', response.text)
        return match.group(1) if match else None
    except:
        return None

def fetch_circuit_stocks():
    api_url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    
    # Configuration for Upper and Lower Circuit scans
    scans_config = {
        "upper_circuit_stocks.json": {
            "val": "1",
            "field": "LiveData.UpperCircuitBreak",
            "web_key": "stocks/market/shares-with-upper-circuit",
            "web_url": "https://dhan.co/stocks/market/shares-with-upper-circuit/"
        },
        "lower_circuit_stocks.json": {
            "val": "1",
            "field": "LiveData.LowerCircuitBreak",
            "web_key": "stocks/market/lower-circuit-stocks",
            "web_url": "https://dhan.co/stocks/market/lower-circuit-stocks/"
        }
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/json"
    }

    build_id = get_build_id()

    for filename, config in scans_config.items():
        print(f"Processing {filename}...")
        success = False
        cleaned_list = []
        
        # --- Attempt 1: SCANX Direct API (Primary) ---
        print(f"  Primary Fetch: ScanX Analytics API...")
        try:
            payload = {
                "data": {
                    "sort": "Mcap", "sorder": "desc", "count": 500,
                    "fields": ["Sym", "DispSym", "Ltp", "PPerchange", "Mcap", "Volume", "High5yr", "Low1Yr", "High1Yr", "Pe", "Pb", "DivYeild"],
                    "params": [
                        {"field": config['field'], "op": "", "val": config['val']},
                        {"field": "OgInst", "op": "", "val": "ES"},
                        {"field": "Seg", "op": "", "val": "E"}
                    ],
                    "pgno": 1
                }
            }
            response = requests.post(api_url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                stocks = response.json().get('data', [])
                for item in stocks:
                    cleaned_list.append({
                        "Symbol": item.get('Sym'), "Name": item.get('DispSym'), "LTP": item.get('Ltp'),
                        "ChangePercent": item.get('PPerchange'), "MarketCap": item.get('Mcap'), "Volume": item.get('Volume'),
                        "High5Yr": item.get('High5yr'), "High1Yr": item.get('High1Yr'), "Low1Yr": item.get('Low1Yr'),
                        "PE": item.get('Pe'), "PB": item.get('Pb'), "DivYield": item.get('DivYeild')
                    })
                print(f"  Successfully found {len(cleaned_list)} stocks via ScanX API.")
                success = True
        except Exception as e:
            print(f"  ScanX API Failed: {e}")

        # --- Attempt 2: Next.js Direct JSON API ---
        if not success and build_id:
            print(f"  Secondary Fetch: Next.js Direct JSON API...")
            try:
                direct_url = f"https://dhan.co/_next/data/{build_id}/{config['web_key']}.json"
                response = requests.get(direct_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    page_props = response.json().get('pageProps', {})
                    # The data structure might be nested in listData or mktData
                    def find_list(obj):
                        if isinstance(obj, list) and len(obj) > 3:
                            if isinstance(obj[0], dict) and ('Sym' in obj[0] or 'sym' in obj[0]): return obj
                        if isinstance(obj, dict):
                            for v in obj.values():
                                res = find_list(v)
                                if res: return res
                        return None
                    
                    raw_list = find_list(page_props)
                    if raw_list:
                        for item in raw_list:
                            cleaned_list.append({
                                "Symbol": item.get('Sym'), "Name": item.get('DispSym'), "LTP": item.get('Ltp'),
                                "ChangePercent": item.get('PPerchange'), "MarketCap": item.get('Mcap'), "Volume": item.get('Volume'),
                                "High5Yr": item.get('High5yr'), "High1Yr": item.get('High1Yr'), "Low1Yr": item.get('Low1Yr'),
                                "PE": item.get('Pe'), "PB": item.get('Pb'), "DivYield": item.get('DivYeild')
                            })
                        print(f"  Successfully found {len(cleaned_list)} stocks via Direct JSON.")
                        success = True
            except Exception as e:
                print(f"  Direct JSON Failed: {e}")

        # --- Attempt 3: BeautifulSoup Fallback ---
        if not success:
            print(f"  Final Fallback: Scraping webpage {config['web_url']}...")
            try:
                response = requests.get(config['web_url'], headers=headers, timeout=10)
                soup = BeautifulSoup(response.text, 'html.parser')
                next_data = soup.find('script', id='__NEXT_DATA__')
                if next_data:
                    data_json = json.loads(next_data.string)
                    def find_list_nested(obj):
                        if isinstance(obj, list) and len(obj) > 3:
                            if isinstance(obj[0], dict) and ('Sym' in obj[0] or 'sym' in obj[0]): return obj
                        if isinstance(obj, dict):
                            for v in obj.values():
                                res = find_list_nested(v)
                                if res: return res
                        return None
                    
                    raw_list = find_list_nested(data_json)
                    if raw_list:
                        for item in raw_list:
                            cleaned_list.append({
                                "Symbol": item.get('Sym'), "Name": item.get('DispSym'), "LTP": item.get('Ltp'),
                                "ChangePercent": item.get('PPerchange'), "MarketCap": item.get('Mcap'), "Volume": item.get('Volume'),
                                "High5Yr": item.get('High5yr'), "High1Yr": item.get('High1Yr'), "Low1Yr": item.get('Low1Yr'),
                                "PE": item.get('Pe'), "PB": item.get('Pb'), "DivYield": item.get('DivYeild')
                            })
                        print(f"  Successfully found {len(cleaned_list)} stocks via Web Scrape.")
                        success = True
            except Exception as e:
                print(f"  Fallback Failed: {e}")

        if success:
            with open(filename, "w") as f:
                json.dump(cleaned_list, f, indent=4)
        else:
            print(f"  Critical failure: Could not fetch {filename}.")

if __name__ == "__main__":
    fetch_circuit_stocks()
