import json
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pipeline_utils import BASE_DIR, get_headers

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "master_isin_map.json")
OUTPUT_DIR = os.path.join(BASE_DIR, "market_news")
MAX_THREADS = 15  # 15 threads to be safe with this API
NEWS_LIMIT = 50   # User requested 50 news items per stock

# Ensure output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_market_news(item):
    """
    Fetches the last 50 market news items for a specific ISIN.
    """
    symbol = item.get("Symbol")
    isin = item.get("ISIN")
    
    if not symbol or not isin:
        return None
        
    output_path = os.path.join(OUTPUT_DIR, f"{symbol}_news.json")
    
    # Simple check to skip if recently fetched (optional, can be removed for full freshness)
    # For now, we fetch fresh every time as news updates frequently
    
    url = "https://news-live.dhan.co/v2/news/getLiveNews"
    
    payload = {
        "categories": ["ALL"],
        "page_no": 0,
        "limit": NEWS_LIMIT,
        "first_news_timeStamp": 0,
        "last_news_timeStamp": 0,
        "news_feed_type": "live",
        "stock_list": [isin],
        "entity_id": ""
    }
    
    headers = get_headers()
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            news_items = data.get("data", {}).get("latest_news", [])
            
            if news_items:
                # Clean and structure the news items
                processed_news = []
                for news in news_items:
                    news_obj = news.get("news_object", {})
                    processed_news.append({
                        "Title": news_obj.get("title", ""),
                        "Summary": news_obj.get("text", ""),
                        "Sentiment": news_obj.get("overall_sentiment", "neutral"),
                        "PublishDate": news.get("publish_date", 0),
                        "Source": news.get("category", "")
                    })
                
                final_output = {"Symbol": symbol, "ISIN": isin, "News": processed_news}
                
                with open(output_path, "w") as f:
                    json.dump(final_output, f, indent=4)
                
                return "success"
            else:
                return "empty"
        elif response.status_code == 429:
             time.sleep(2) # Backoff
             return "rate_limit"
        else:
            return f"http_{response.status_code}"
            
    except Exception as e:
        return "error"

def main():
    print(f"Loading ISIN mapping from {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, "r") as f:
            stock_list = json.load(f)
    except Exception as e:
        print(f"Error: Could not load {INPUT_FILE}: {e}")
        return

    total = len(stock_list)
    print(f"Starting Market News Fetch (Limit: {NEWS_LIMIT} items) for {total} stocks...")
    
    success_count = 0
    empty_count = 0
    error_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_stock = {executor.submit(fetch_market_news, item): item["Symbol"] for item in stock_list}
        
        count = 0
        for future in as_completed(future_to_stock):
            count += 1
            res = future.result()
            
            if res == "success": success_count += 1
            elif res == "empty": empty_count += 1
            else: error_count += 1
            
            if count % 50 == 0 or count == total:
                elapsed = time.time() - start_time
                print(f"[{count}/{total}] | Success: {success_count} | Empty: {empty_count} | Errors: {error_count} | Elapsed: {elapsed:.1f}s")

    print("\n--- Final Report ---")
    print(f"Total Time: {time.time() - start_time:.1f}s")
    print(f"News Found: {success_count} stocks | No News: {empty_count} | Errors: {error_count}")
    print(f"Data saved to: {OUTPUT_DIR}/")

if __name__ == "__main__":
    main()
