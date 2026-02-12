import requests
import json
import time
from pipeline_utils import get_headers

def fetch_fundamental_data():
    master_map_file = "master_isin_map.json"
    api_url = "https://open-web-scanx.dhan.co/scanx/fundamental"
    output_file = "fundamental_data.json"
    
    headers = get_headers()

    # 1. Load ISINs from Master Map
    try:
        with open(master_map_file, "r") as f:
            master_map = json.load(f)
    except FileNotFoundError:
        print(f"Error: {master_map_file} not found. Please run 'fetch_dhan_data.py' first.")
        return

    # Create a lookup dictionary for ISIN -> {Symbol, Name}
    isin_lookup = {item['ISIN']: {"Symbol": item.get("Symbol"), "Name": item.get("Name")} for item in master_map if item.get('ISIN')}
    
    all_isins = list(isin_lookup.keys())
    total_isins = len(all_isins)
    print(f"Loaded {total_isins} ISINs from master map.")

    # 2. Chunk ISINs into batches of 100
    batch_size = 100
    all_fundamental_data = []
    
    for i in range(0, total_isins, batch_size):
        batch_isins = all_isins[i:i + batch_size]
        print(f"Fetching batch {i//batch_size + 1}: {len(batch_isins)} ISINs ({i}-{i+len(batch_isins)})...")
        
        payload = {
            "data": {
                "isins": batch_isins
            }
        }
        
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'success':
                    # The API returns a list of objects in 'data'
                    batch_results = data.get('data', [])
                    if batch_results:
                        # Enrich fetched data with Symbol and Name via ISIN lookup
                        for item in batch_results:
                            item_isin = item.get('isin')
                            if item_isin and item_isin in isin_lookup:
                                item['Symbol'] = isin_lookup[item_isin]['Symbol']
                                item['Name'] = isin_lookup[item_isin]['Name']
                        
                        all_fundamental_data.extend(batch_results)
                        print(f"  Success: Received {len(batch_results)} records.")
                    else:
                        print("  Warning: No data returned for this batch.")
                else:
                    print(f"  API Error: {data.get('message')}")
            else:
                print(f"  HTTP Error: {response.status_code}")
                
        except Exception as e:
            print(f"  Exception fetching batch: {e}")
            
        # Be polite to the server
        time.sleep(0.5)

    # 3. Save Consolidated Data
    if all_fundamental_data:
        with open(output_file, "w") as f:
            json.dump(all_fundamental_data, f, indent=4)
        print(f"\nSuccessfully saved fundamental data for {len(all_fundamental_data)} securities to {output_file}")
    else:
        print("\nFailed to fetch any fundamental data.")

if __name__ == "__main__":
    fetch_fundamental_data()
