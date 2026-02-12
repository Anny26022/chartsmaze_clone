#!/bin/bash

# TradingView Scanner API - Fetch first 100 stocks
# This script demonstrates how to use curl to fetch TradingView data

echo "Fetching TradingView data using curl..."
echo "================================"

curl -X POST 'https://scanner.tradingview.com/india/scan' \
-H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \
-H 'Accept: application/json, text/plain, */*' \
-H 'Accept-Language: en-US,en;q=0.9' \
-H 'Content-Type: application/json' \
-H 'Referer: https://www.tradingview.com/' \
-H 'Origin: https://www.tradingview.com' \
--data-raw '{
  "filter": [
    {"left": "exchange", "operation": "in_range", "right": ["BSE", "NSE"]},
    {"left": "type", "operation": "in_range", "right": ["stock"]}
  ],
  "options": {"lang": "en"},
  "symbols": {},
  "columns": [
    "name",
    "description", 
    "logoid",
    "update_mode",
    "type",
    "close",
    "pricescale",
    "minmov",
    "fractional", 
    "change",
    "change_abs",
    "volume",
    "market_cap_basic",
    "sector",
    "industry",
    "exchange",
    "subtype",
    "country",
    "fundamentals"
  ],
  "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
  "range": [0, 100],
  "markets": ["india"]
}' > tradingview_response.json

echo ""
echo "Response saved to tradingview_response.json"
echo "Total data size: $(wc -c < tradingview_response.json) bytes"
echo ""
echo "First few lines of response:"
head -20 tradingview_response.json
