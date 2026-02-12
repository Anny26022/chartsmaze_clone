import requests
import json
from datetime import datetime, timedelta
import math
from pipeline_utils import get_headers

def fetch_bulk_block_deals():
    # Dhan API endpoint for bulk/block deals
    url = "https://ow-static-scanx.dhan.co/staticscanx/deal"
    headers = get_headers()

    # API Limitation: "start date and end date difference is more than 240 hours" (10 days)
    # We want the last 30 days. We will fetch in 3 separate 10-day chunks.
    
    end_date_ref = datetime.now() # Today
    all_raw_deals = []
    
    # Loop 3 times for 3 chunks: [0-9 days], [10-19 days], [20-29 days] ago
    for i in range(3):
        days_offset_end = i * 10 
        days_offset_start = days_offset_end + 9
        
        chunk_end_date = end_date_ref - timedelta(days=days_offset_end)
        chunk_start_date = end_date_ref - timedelta(days=days_offset_start)
        
        start_str = chunk_start_date.strftime("%d-%m-%Y")
        end_str = chunk_end_date.strftime("%d-%m-%Y")
        
        print(f"Fetching deals for chunk {i+1}/3: {start_str} to {end_str}...")
        
        page_no = 1
        max_pages = 1 # Will be updated from first response
        
        while page_no <= max_pages:
            payload = {
                "data": {
                    "startdate": start_str,
                    "enddate": end_str,
                    "defaultpage": "N",
                    "pageno": page_no,
                    "pagecount": 50
                }
            }
            
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    deals = data.get('data', [])
                    
                    if not deals:
                        print(f"  No deals found on page {page_no}.")
                        break
                        
                    all_raw_deals.extend(deals)
                    
                    # Update max pages from API response
                    total_count = data.get('totalcount', 0)
                    if total_count > 0:
                        max_pages = math.ceil(total_count / 50)
                    else:
                        max_pages = 1
                        
                    print(f"  Fetched page {page_no}/{max_pages} ({len(deals)} items)")
                    page_no += 1
                else:
                    print(f"  Error fetching page {page_no}: Status {response.status_code}")
                    break
            except Exception as e:
                print(f"  Exception fetching page {page_no}: {e}")
                break

    # Deduplicate deals based on unique composite key
    # (Symbol + Date + Quantity + Price + Buyer/Seller)
    unique_deals_map = {}
    for d in all_raw_deals:
        # Create a unique key for each deal to avoid duplicates if chunks overlap or pagination is weird
        key = f"{d.get('sym')}_{d.get('date')}_{d.get('qty')}_{d.get('avgprice')}_{d.get('bs')}_{d.get('cname')}"
        unique_deals_map[key] = d

    sorted_deals = sorted(list(unique_deals_map.values()), key=lambda x: x.get('date', ''), reverse=True)

    if sorted_deals:
        output_file = "bulk_block_deals.json"
        with open(output_file, "w") as f:
            json.dump(sorted_deals, f, indent=4)
        print(f"Successfully saved {len(sorted_deals)} unique bulk/block deals to {output_file}")
    else:
        print("No bulk/block deals found for the last 30 days.")

if __name__ == "__main__":
    fetch_bulk_block_deals()
