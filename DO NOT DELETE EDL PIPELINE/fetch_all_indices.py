import requests
import json
from pipeline_utils import get_headers

def fetch_all_indices():
    """
    Fetches strictly NSE Indices using the ScanX Analytics API.
    This is the live source-of-truth used by the Dhan terminal.
    """
    print("Fetching NSE Indices via ScanX Pro API...")
    
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    
    # Payload designed to get all indices with their live markers
    payload = {
        "data": {
            "sort": "Sym", "sorder": "asc", "count": 500,
            "fields": [
                "Sym", "DispSym", "Sid", "Exch", "Seg", "Inst", 
                "Ltp", "Pchange", "PPerchange", "High1Yr", "Low1Yr"
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
            
            # Since you want STRICTLY NSE Indices, we filter for common NSE index patterns
            # or exclude known BSE/Global ones if necessary. 
            # Dhan's ScanX primarily contains NSE indices for 'Exch: IDX'.
            
            final_indices = []
            for item in raw_data:
                final_indices.append({
                    "IndexName": item.get('DispSym'),
                    "Symbol": item.get('Sym'),
                    "IndexID": item.get('Sid'),
                    "Exchange": item.get('Exch'),
                    "Segment": item.get('Seg'),
                    "Instrument": item.get('Inst'),
                    "Ltp": item.get('Ltp'),
                    "Chng": item.get('Pchange'),
                    "PChng": item.get('PPerchange'),
                    "52W_High": item.get('High1Yr'),
                    "52W_Low": item.get('Low1Yr')
                })
            
            print(f"Successfully identified {len(final_indices)} Indices with Live LTP.")
            
            output_file = "all_indices_list.json"
            with open(output_file, 'w') as f:
                json.dump(final_indices, f, indent=4)
            print(f"Saved to {output_file}")
            return True
        else:
            print(f"API Error: Status {response.status_code}")
            return False
    except Exception as e:
        print(f"Critical API Failure: {e}")
        return False

if __name__ == "__main__":
    fetch_all_indices()
