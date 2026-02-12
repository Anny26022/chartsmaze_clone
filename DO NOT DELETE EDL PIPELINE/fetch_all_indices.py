import requests
import json
from pipeline_utils import get_headers

def fetch_all_indices():
    """
    Fetches strictly NSE Indices using ScanX Pro API.
    Captures Live LTP, OHLC, and TODAY'S VOLUME (from technical fields).
    """
    print("Fetching NSE Indices & Today's Volume via ScanX Pro API...")
    
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    
    payload = {
        "data": {
            "sort": "Sym", "sorder": "asc", "count": 500,
            "fields": [
                "Sym", "DispSym", "Sid", "Exch", "Seg", "Inst", 
                "Open", "High", "Low", "Ltp", "Pchange", "PPerchange", 
                "High1Yr", "Low1Yr", "Min1TotalVolPrevCandle" # <--- Cumulative Today Volume
            ],
            "params": [
                {"field": "Inst", "op": "", "val": "IDX"},
                {"field": "Exch", "op": "", "val": "IDX"}
            ],
            "pgno": 1
        }
    }

    try:
        response = requests.post(url, json=payload, headers=get_headers(), timeout=15)
        if response.status_code == 200:
            raw_data = response.json().get('data', [])
            
            final_indices = []
            for item in raw_data:
                # Get volume, ensure it is positive (some specific fields might have overflow -1 values)
                vol = item.get('Min1TotalVolPrevCandle', 0)
                if isinstance(vol, (int, float)) and vol < 0: vol = 0

                final_indices.append({
                    "IndexName": item.get('DispSym'),
                    "Symbol": item.get('Sym'),
                    "IndexID": item.get('Sid'),
                    "Exchange": item.get('Exch'),
                    "Segment": item.get('Seg'),
                    "Instrument": item.get('Inst'),
                    "Open": item.get('Open'),
                    "High": item.get('High'),
                    "Low": item.get('Low'),
                    "Ltp": item.get('Ltp'),
                    "Chng": item.get('Pchange'),
                    "PChng": item.get('PPerchange'),
                    "Volume": vol, # TODAY'S LIVE VOLUME
                    "52W_High": item.get('High1Yr'),
                    "52W_Low": item.get('Low1Yr')
                })
            
            print(f"Successfully identified {len(final_indices)} Indices with Today's Volume.")
            
            output_file = "all_indices_list.json"
            with open(output_file, 'w') as f:
                json.dump(final_indices, f, indent=4)
            return True
        else:
            print(f"API Error: Status {response.status_code}")
            return False
    except Exception as e:
        print(f"Critical API Failure: {e}")
        return False

if __name__ == "__main__":
    fetch_all_indices()
