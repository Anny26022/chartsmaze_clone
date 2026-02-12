import requests
import json
from bs4 import BeautifulSoup
from pipeline_utils import get_headers

def get_indices_from_url(url):
    """Scrape indices from a Dhan page's __NEXT_DATA__."""
    headers = get_headers()
    indices = []
    try:
        response = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        next_data = soup.find('script', id='__NEXT_DATA__')
        if next_data:
            data = json.loads(next_data.string)
            raw_list = data.get('props', {}).get('pageProps', {}).get('listData', [])
            for item in raw_list:
                indices.append({
                    "IndexName": item.get('DispSym'), "Symbol": item.get('Sym'), "IndexID": item.get('Sid'),
                    "Exchange": item.get('Exch', 'IDX'), "Segment": item.get('Seg', 'I'), "Instrument": item.get('Inst', 'IDX'),
                    "Ltp": item.get('Ltp'), "Chng": item.get('Pchange'), "PChng": item.get('PPerchange')
                })
    except Exception as e:
        print(f"Error scraping {url}: {e}")
    return indices

def fetch_all_indices():
    print("Fetching STRICTLY NSE indices from Dhan source...")
    
    # Target ONLY the NSE Indices page
    nse_ind = get_indices_from_url("https://dhan.co/all-nse-indices/")
    print(f"  NSE Indices found: {len(nse_ind)}")
    
    # Deduplicate by IndexID just in case
    master_map = {}
    for item in nse_ind:
        sid = item.get("IndexID")
        if sid:
            master_map[sid] = item
            
    final_list = list(master_map.values())
    print(f"Total NSE Indices Identified: {len(final_list)}")
    
    output_file = "all_indices_list.json"
    with open(output_file, 'w') as f:
        json.dump(final_list, f, indent=4)
        
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    fetch_all_indices()
