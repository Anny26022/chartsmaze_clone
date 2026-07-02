import requests
import sys
from datetime import datetime, timedelta
import math
from pipeline_utils import get_headers, save_json


API_URL = "https://ow-static-scanx.dhan.co/staticscanx/deal"
PAGE_SIZE = 50
CHUNK_DAYS = 10
CHUNK_COUNT = 3
OUTPUT_FILE = "bulk_block_deals.json"


def build_payload(start_str, end_str, page_no):
    return {
        "data": {
            "startdate": start_str,
            "enddate": end_str,
            "defaultpage": "N",
            "pageno": page_no,
            "pagecount": PAGE_SIZE,
        }
    }


def date_chunks(end_date_ref):
    for index in range(CHUNK_COUNT):
        days_offset_end = index * CHUNK_DAYS
        days_offset_start = days_offset_end + CHUNK_DAYS - 1
        chunk_end_date = end_date_ref - timedelta(days=days_offset_end)
        chunk_start_date = end_date_ref - timedelta(days=days_offset_start)
        yield index + 1, chunk_start_date.strftime("%d-%m-%Y"), chunk_end_date.strftime("%d-%m-%Y")


def dedupe_deals(deals):
    unique_deals_map = {}
    for deal in deals:
        key = f"{deal.get('sym')}_{deal.get('date')}_{deal.get('qty')}_{deal.get('avgprice')}_{deal.get('bs')}_{deal.get('cname')}"
        unique_deals_map[key] = deal
    return sorted(unique_deals_map.values(), key=lambda x: x.get("date", ""), reverse=True)

def fetch_bulk_block_deals():
    headers = get_headers()

    # API Limitation: "start date and end date difference is more than 240 hours" (10 days)
    # We want the last 30 days. We will fetch in 3 separate 10-day chunks.
    all_raw_deals = []

    for chunk_number, start_str, end_str in date_chunks(datetime.now()):
        print(f"Fetching deals for chunk {chunk_number}/{CHUNK_COUNT}: {start_str} to {end_str}...")
        
        page_no = 1
        max_pages = 1 # Will be updated from first response
        
        while page_no <= max_pages:
            try:
                response = requests.post(API_URL, json=build_payload(start_str, end_str, page_no), headers=headers, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    deals = data.get('data', [])
                    
                    if not deals:
                        print(f"  No deals found on page {page_no}.")
                        break
                        
                    all_raw_deals.extend(deals)
                    
                    # Update max pages from API response
                    total_count = data.get('totalcount', 0)
                    max_pages = math.ceil(total_count / PAGE_SIZE) if total_count > 0 else 1
                        
                    print(f"  Fetched page {page_no}/{max_pages} ({len(deals)} items)")
                    page_no += 1
                else:
                    print(f"  Error fetching page {page_no}: Status {response.status_code}")
                    break
            except Exception as e:
                print(f"  Exception fetching page {page_no}: {e}")
                break

    sorted_deals = dedupe_deals(all_raw_deals)

    if sorted_deals:
        save_json(OUTPUT_FILE, sorted_deals)
        print(f"Successfully saved {len(sorted_deals)} unique bulk/block deals to {OUTPUT_FILE}")
        return True

    print("No bulk/block deals found for the last 30 days.")
    return True

if __name__ == "__main__":
    sys.exit(0 if fetch_bulk_block_deals() else 1)
