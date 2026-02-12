import requests
import json
import re

def get_build_id():
    url = "https://dhan.co/nse-fno-lot-size/"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        match = re.search(r'"buildId":"([^"]+)"', response.text)
        return match.group(1) if match else None
    except:
        return None

def fetch_fno_lot_sizes():
    build_id = get_build_id()
    direct_json_url = f"https://dhan.co/_next/data/{build_id}/nse-fno-lot-size.json"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    print(f"Primary Fetch: Direct JSON via Next.js Data API...")
    instrument_list = []
    try:
        response = requests.get(direct_json_url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            instrument_list = data.get('pageProps', {}).get('listData', [])
        
        if not instrument_list:
            print("Falling back to BeautifulSoup extraction...")
            from bs4 import BeautifulSoup
            web_url = "https://dhan.co/nse-fno-lot-size/"
            response = requests.get(web_url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            next_data = soup.find('script', id='__NEXT_DATA__')
            if next_data:
                data_json = json.loads(next_data.string)
                # Helper to find the flat list in props
                def find_data_list(obj):
                    if isinstance(obj, list) and len(obj) > 10:
                        if isinstance(obj[0], dict) and ('sym' in obj[0] or 'symbol' in obj[0]): return obj
                    if isinstance(obj, dict):
                        for v in obj.values():
                            res = find_data_list(v)
                            if res: return res
                    return None
                instrument_list = find_data_list(data_json.get('props', {}))

        if not instrument_list:
            print("ERROR: Could not locate lot size data.")
            return

        final_list = []
        for item in instrument_list:
            symbol = item.get('sym') or item.get('symbol')
            name = item.get('disp') or item.get('companyName')
            fo_contracts = item.get('fo_dt', [])
            
            lots = {}
            for i, contract in enumerate(fo_contracts):
                contract_sym = contract.get('sym', '')
                try:
                    month_label = contract_sym.split('-')[1]
                except:
                    month_label = f"Month_{i+1}"
                lots[f"Lot_{month_label}"] = contract.get('ls')
            
            entry = {"Symbol": symbol, "Name": name}
            entry.update(lots)
            final_list.append(entry)
        
        output_file = "fno_lot_sizes_cleaned.json"
        with open(output_file, 'w') as f:
            json.dump(final_list, f, indent=4)
            
        print(f"Successfully extracted {len(final_list)} instruments via Direct JSON.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_fno_lot_sizes()
