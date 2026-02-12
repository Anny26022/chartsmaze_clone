import requests
import json
import re

def get_build_id():
    """Dynamically fetch the Next.js buildId from the Dhan homepage."""
    url = "https://dhan.co/all-indices/" # Any page works
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        match = re.search(r'"buildId":"([^"]+)"', response.text)
        return match.group(1) if match else None
    except:
        return None

def fetch_fno_expiry_calendar():
    build_id = get_build_id()
    if not build_id:
        print("Could not find dynamic buildId. Falling back to static extraction...")
        # (I could keep the BeautifulSoup fallback here if I wanted, 
        # but let's try the primary direct JSON first)
    
    direct_json_url = f"https://dhan.co/_next/data/{build_id}/fno-expiry-calendar.json"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    print(f"Primary Fetch: Direct JSON via Next.js Data API...")
    try:
        response = requests.get(direct_json_url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            expiry_data_raw = data.get('pageProps', {}).get('expiryData', {}).get('data', [])
        else:
            print(f"Direct JSON failed (Status {response.status_code}).")
            expiry_data_raw = []

        if not expiry_data_raw:
            print("Falling back to BeautifulSoup extraction...")
            from bs4 import BeautifulSoup
            web_url = "https://dhan.co/fno-expiry-calendar/"
            response = requests.get(web_url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            next_data = soup.find('script', id='__NEXT_DATA__')
            if next_data:
                data_json = json.loads(next_data.string)
                expiry_data_raw = data_json.get('props', {}).get('pageProps', {}).get('expiryData', {}).get('data', [])

        if not expiry_data_raw:
            print("ERROR: Could not locate expiry data from any source.")
            return

        flattened_data = []
        for exchange_data in expiry_data_raw:
            exch = exchange_data.get('exch')
            seg = exchange_data.get('seg')
            for exp_group in exchange_data.get('exps', []):
                inst_type = exp_group.get('inst')
                for item in exp_group.get('explst', []):
                    flattened_data.append({
                        "Exchange": exch,
                        "Segment": seg,
                        "InstrumentType": inst_type,
                        "SymbolName": item.get('symbolName'),
                        "ExpiryDate": item.get('expdate'),
                        "UnderlyingSecID": item.get('underlyingSecID')
                    })

        output_file = "fno_expiry_calendar.json"
        with open(output_file, 'w') as f:
            json.dump(flattened_data, f, indent=4)
        
        print(f"Successfully processed {len(flattened_data)} expiry entries.")
        print(f"Data saved to {output_file}")

    except Exception as e:
        print(f"FAILED to fetch expiry calendar: {e}")

if __name__ == "__main__":
    fetch_fno_expiry_calendar()
