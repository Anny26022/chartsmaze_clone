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

def fetch_surveillance_lists():
    # Google Spreadsheet Gviz API (The most direct source)
    spreadsheet_base_url = "https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:json&gid="
    
    # Configuration for NSE surveillance lists
    lists_config = {
        "nse_asm_list.json": {
            "gid": "290894275",
            "web_url": "https://dhan.co/nse-asm-list/",
            "data_key": "nse-asm-list"
        },
        "nse_gsm_list.json": {
            "gid": "1525483995",
            "web_url": "https://dhan.co/nse-gsm-list/",
            "data_key": "nse-gsm-list"
        }
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    build_id = get_build_id()

    for filename, config in lists_config.items():
        gid = config['gid']
        web_url = config['web_url']
        data_key = config['data_key']
        success = False
        cleaned_list = []
        
        # --- Attempt 1: Direct Google Sheet (Gviz API) ---
        print(f"Primary Fetch: Gviz API (Spreadsheet) for {filename}...")
        try:
            url = f"{spreadsheet_base_url}{gid}"
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            text = response.text
            match = re.search(r'setResponse\((.*)\);', text)
            if match:
                data = json.loads(match.group(1))
                rows = data.get('table', {}).get('rows', [])
                
                for row in rows:
                    c = row.get('c', [])
                    if len(c) >= 5:
                        symbol = c[1].get('v') if c[1] else None
                        name = c[2].get('v') if c[2] else None
                        isin = c[3].get('v') if c[3] else None
                        stage = c[4].get('v') if c[4] else None
                        
                        if symbol == "Symbol" or not symbol:
                            continue
                            
                        cleaned_list.append({
                            "Symbol": str(symbol),
                            "Name": str(name),
                            "ISIN": str(isin),
                            "Stage": str(stage)
                        })

                if cleaned_list:
                    print(f"Successfully saved {len(cleaned_list)} items via Gviz.")
                    success = True
        except Exception as e:
            print(f"Gviz Fetch Failed: {e}")

        # --- Attempt 2: Next.js Direct JSON API ---
        if not success and build_id:
            print(f"Secondary Fetch: Next.js Direct JSON API for {filename}...")
            try:
                direct_url = f"https://dhan.co/_next/data/{build_id}/{data_key}.json"
                response = requests.get(direct_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    # Search for list in Next.js props
                    data_json = response.json()
                    page_props = data_json.get('pageProps', {})
                    
                    def find_list(obj):
                        if isinstance(obj, list) and len(obj) > 3:
                            if isinstance(obj[0], dict) and ('sym' in obj[0] or 'Sym' in obj[0]): return obj
                        if isinstance(obj, dict):
                            for v in obj.values():
                                res = find_list(v)
                                if res: return res
                        return None
                    
                    raw_list = find_list(page_props)
                    if raw_list:
                        for item in raw_list:
                            cleaned_list.append({
                                "Symbol": item.get('sym') or item.get('Sym'),
                                "Name": item.get('DispSym') or item.get('Name'),
                                "ISIN": item.get('isin') or item.get('Isin'),
                                "Stage": item.get('asmStage') or item.get('gsmStage') or item.get('Stage')
                            })
                        if cleaned_list:
                            print(f"Successfully saved {len(cleaned_list)} items via Direct JSON.")
                            success = True
            except Exception as e:
                print(f"Direct JSON Failed: {e}")

        # --- Attempt 3: Final Fallback (BeautifulSoup Scrape) ---
        if not success:
            print(f"Final Fallback: Scraping webpage {web_url}...")
            try:
                # Try the direct page or the iframe index.html
                for target_url in [web_url.rstrip("/") + "/index.html", web_url]:
                    response = requests.get(target_url, headers=headers, timeout=10)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        script = soup.find('script', id='__NEXT_DATA__')
                        if script:
                            data_json = json.loads(script.string)
                            def find_list_nested(obj):
                                if isinstance(obj, list) and len(obj) > 3:
                                    if isinstance(obj[0], dict) and ('sym' in obj[0] or 'Sym' in obj[0]): return obj
                                if isinstance(obj, dict):
                                    for v in obj.values():
                                        res = find_list_nested(v)
                                        if res: return res
                                return None
                            raw_list = find_list_nested(data_json.get('props', {}))
                            if raw_list:
                                for item in raw_list:
                                    cleaned_list.append({
                                        "Symbol": item.get('sym') or item.get('Sym'),
                                        "Name": item.get('DispSym') or item.get('Name'),
                                        "ISIN": item.get('isin') or item.get('Isin'),
                                        "Stage": item.get('asmStage') or item.get('gsmStage') or item.get('Stage')
                                    })
                                if cleaned_list:
                                    print(f"Successfully saved {len(cleaned_list)} items via Web Scrape.")
                                    success = True
                                    break
            except Exception as e:
                print(f"Fallback Failed: {e}")

        if success:
            with open(filename, "w") as f:
                json.dump(cleaned_list, f, indent=4)
        else:
            print(f"Critical failure: Could not fetch {filename} from any source.")

if __name__ == "__main__":
    fetch_surveillance_lists()
