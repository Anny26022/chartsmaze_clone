import requests
import json
import re
from bs4 import BeautifulSoup

def get_build_id():
    url = "https://dhan.co/all-indices/"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        match = re.search(r'"buildId":"([^"]+)"', response.text)
        return match.group(1) if match else None
    except:
        return None

def fetch_all_indices():
    api_url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    web_url = "https://dhan.co/all-indices/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/json"
    }

    success = False
    cleaned_indices = []

    # --- Attempt 1: SCANX Direct API (Primary) ---
    print(f"Primary Fetch: ScanX Analytics API...")
    try:
        payload = {
            "data": {
                "sort": "Sym", "sorder": "asc", "count": 500,
                "fields": ["Sym", "DispSym", "Sid", "Exch", "Seg", "Inst", "Ltp", "Pchange", "PPerchange"],
                "params": [{"field": "Inst", "op": "", "val": "IDX"}, {"field": "Exch", "op": "", "val": "IDX"}],
                "pgno": 1
            }
        }
        response = requests.post(api_url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            raw_indices = response.json().get('data', [])
            for item in raw_indices:
                cleaned_indices.append({
                    "IndexName": item.get('DispSym'), "Symbol": item.get('Sym'), "IndexID": item.get('Sid'),
                    "Exchange": item.get('Exch'), "Segment": item.get('Seg'), "Instrument": item.get('Inst'),
                    "Ltp": item.get('Ltp'), "Chng": item.get('Pchange'), "PChng": item.get('PPerchange')
                })
            print(f"Successfully found {len(cleaned_indices)} indices via ScanX API.")
            success = True
    except Exception as e: print(f"ScanX API Failed: {e}")

    # --- Attempt 2: Direct JSON via Next.js Data API ---
    if not success:
        print(f"Secondary Fetch: Next.js Direct JSON API...")
        build_id = get_build_id()
        if build_id:
            try:
                direct_url = f"https://dhan.co/_next/data/{build_id}/all-indices.json"
                response = requests.get(direct_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    indices = response.json().get('pageProps', {}).get('listData', [])
                    for item in indices:
                        cleaned_indices.append({
                            "IndexName": item.get('DispSym'), "Symbol": item.get('Sym'), "IndexID": item.get('Sid'),
                            "Exchange": item.get('Exch'), "Segment": item.get('Seg'), "Instrument": item.get('Inst'),
                            "Ltp": item.get('Ltp'), "Chng": item.get('Pchange'), "PChng": item.get('PPerchange')
                        })
                    print(f"Successfully found {len(cleaned_indices)} indices via Direct JSON.")
                    success = True
            except Exception as e: print(f"Direct JSON Failed: {e}")

    # --- Attempt 3: BeautifulSoup Fallback ---
    if not success:
        print(f"Final Fallback: BeautifulSoup Web Scrape...")
        try:
            response = requests.get(web_url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            next_data = soup.find('script', id='__NEXT_DATA__')
            if next_data:
                indices = json.loads(next_data.string).get('props', {}).get('pageProps', {}).get('listData', [])
                for item in indices:
                    cleaned_indices.append({
                        "IndexName": item.get('DispSym'), "Symbol": item.get('Sym'), "IndexID": item.get('Sid'),
                        "Exchange": item.get('Exch'), "Segment": item.get('Seg'), "Instrument": item.get('Inst'),
                        "Ltp": item.get('Ltp'), "Chng": item.get('Pchange'), "PChng": item.get('PPerchange')
                    })
                print(f"Successfully found {len(cleaned_indices)} indices via Web Scrape.")
                success = True
        except Exception as e: print(f"Fallback Failed: {e}")

    if success:
        with open("all_indices_list.json", 'w') as f: json.dump(cleaned_indices, f, indent=4)
    else: print("Critical failure: Could not fetch indices.")

if __name__ == "__main__":
    fetch_all_indices()
