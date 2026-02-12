#!/usr/bin/env python3
"""
TradingView India Stock Scanner API Scraper
Fetches all stock data from TradingView scanner API with pagination support.
"""

import requests
import json
import time
import csv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TradingViewScanner:
    def __init__(self):
        self.base_url = "https://scanner.tradingview.com/india/scan"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.tradingview.com/',
            'Origin': 'https://www.tradingview.com'
        })
        
        self.scraped_data = []
        self.max_items_per_request = 100  # TradingView typical limit
        
    def build_payload(self, offset=0, limit=100):
        """Build the API payload with proper filters and sorting - Exact TradingView format"""
        payload = {
            "columns": [
                "name",
                "description",
                "logoid",
                "update_mode",
                "type",
                "typespecs",
                "close",
                "pricescale",
                "minmov",
                "fractional",
                "minmove2",
                "currency",
                "change",
                "volume",
                "relative_volume_10d_calc",
                "market_cap_basic",
                "fundamental_currency_code",
                "price_earnings_ttm",
                "earnings_per_share_diluted_ttm",
                "earnings_per_share_diluted_yoy_growth_ttm",
                "dividends_yield_current",
                "sector.tr",
                "market",
                "sector",
                "AnalystRating",
                "AnalystRating.tr",
                "exchange"
            ],
            "filter": [
                {"left": "exchange", "operation": "in_range", "right": ["BSE"]},
                {"left": "is_primary", "operation": "equal", "right": True}
            ],
            "filter2": {
                "operator": "and",
                "operands": [
                    {
                        "operation": {
                            "operator": "or",
                            "operands": [
                                {
                                    "operation": {
                                        "operator": "and",
                                        "operands": [
                                            {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                                            {"expression": {"left": "exchange", "operation": "in_range", "right": ["BSE"]}}
                                        ]
                                    }
                                },
                                {
                                    "operation": {
                                        "operator": "and",
                                        "operands": [
                                            {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                                            {"expression": {"left": "exchange", "operation": "in_range", "right": ["NSE"]}}
                                        ]
                                    }
                                },
                                {
                                    "operation": {
                                        "operator": "and",
                                        "operands": [
                                            {"expression": {"left": "type", "operation": "equal", "right": "dr"}}
                                        ]
                                    }
                                },
                                {
                                    "operation": {
                                        "operator": "and",
                                        "operands": [
                                            {"expression": {"left": "type", "operation": "equal", "right": "fund"}},
                                            {"expression": {"left": "exchange", "operation": "in_range", "right": ["BSE"]}}
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            },
            "ignore_unknown_fields": False,
            "markets": ["india"],
            "options": {"lang": "en"},
            "range": [offset, offset + limit],
            "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
            "symbols": {}
        }
        return payload
    
    def fetch_stocks_data(self, offset=0, limit=100):
        """Fetch stock data from TradingView scanner"""
        try:
            payload = self.build_payload(offset, limit)
            
            logger.info(f"Fetching stocks {offset} to {offset + limit}")
            
            response = self.session.post(
                self.base_url,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                logger.error(f"Error {response.status_code}: {response.text}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return None
    
    def parse_stock_data(self, raw_data):
        """Parse raw API response and extract stock information"""
        stocks = []
        
        if not raw_data or 'data' not in raw_data:
            return stocks
        
        for item in raw_data['data']:
            try:
                symbol_full = item.get('s', '')
                
                # Extract exchange and symbol from format like "BSE:SOMEIT" or "NSE:MARUTI"
                if ':' in symbol_full:
                    exchange, symbol = symbol_full.split(':', 1)
                else:
                    continue
                
                # Skip ETFs and other non-stock instruments
                if exchange in ['NSE', 'BSE']:
                    data_fields = item.get('d', [])
                    if len(data_fields) >= 14:  # Minimum required fields
                        stocks.append({
                            'symbol': symbol,
                            'exchange': exchange,
                            'name': data_fields[0] if len(data_fields) > 0 else '',
                            'description': data_fields[1] if len(data_fields) > 1 else '',
                            'logoid': data_fields[2] if len(data_fields) > 2 else '',
                            'update_mode': data_fields[3] if len(data_fields) > 3 else '',
                            'type': data_fields[4] if len(data_fields) > 4 else '',
                            'typespecs': data_fields[5] if len(data_fields) > 5 else '',
                            'close': data_fields[6] if len(data_fields) > 6 else '',
                            'pricescale': data_fields[7] if len(data_fields) > 7 else '',
                            'minmov': data_fields[8] if len(data_fields) > 8 else '',
                            'fractional': data_fields[9] if len(data_fields) > 9 else '',
                            'minmove2': data_fields[10] if len(data_fields) > 10 else '',
                            'currency': data_fields[11] if len(data_fields) > 11 else '',
                            'change': data_fields[12] if len(data_fields) > 12 else '',
                            'volume': data_fields[13] if len(data_fields) > 13 else '',
                            'relative_volume_10d_calc': data_fields[14] if len(data_fields) > 14 else '',
                            'market_cap_basic': data_fields[15] if len(data_fields) > 15 else '',
                            'free_currency_code': data_fields[16] if len(data_fields) > 16 else '',
                            'price_earnings_ttm': data_fields[17] if len(data_fields) > 17 else '',
                            'earnings_per_share_diluted_ttm': data_fields[18] if len(data_fields) > 18 else '',
                            'earnings_per_share_diluted_yoy_growth_ttm': data_fields[19] if len(data_fields) > 19 else '',
                            'dividends_yield_current': data_fields[20] if len(data_fields) > 20 else '',
                            'sector_tr': data_fields[21] if len(data_fields) > 21 else '',
                            'market': data_fields[22] if len(data_fields) > 22 else '',
                            'sector': data_fields[23] if len(data_fields) > 23 else '',
                            'AnalystRating': data_fields[24] if len(data_fields) > 24 else '',
                            'AnalystRating_tr': data_fields[25] if len(data_fields) > 25 else '',
                            'exchange_field': data_fields[26] if len(data_fields) > 26 else '',
                            'full_symbol': symbol_full
                        })
            
            except (IndexError, ValueError) as e:
                logger.warning(f"Error parsing stock data: {e}")
                continue
        
        return stocks
    
    def scrape_all_stocks(self, max_iterations=100):
        """Scrape all available stocks with pagination"""
        offset = 0
        limit = self.max_items_per_request
        iteration = 0
        
        logger.info("Starting TradingView stock scanning...")
        
        while iteration < max_iterations:
            logger.info(f"Iteration {iteration + 1}: Fetching items {offset} to {offset + limit}")
            
            raw_data = self.fetch_stocks_data(offset, limit)
            
            if not raw_data:
                logger.error(f"No data received at offset {offset}")
                break
            
            stocks = self.parse_stock_data(raw_data)
            
            if not stocks:
                logger.info("No more stocks found, stopping...")
                break
            
            self.scraped_data.extend(stocks)
            
            logger.info(f"Fetch and parse complete. Found {len(stocks)} stocks. Total: {len(self.scraped_data)}")
            
            # Check if we got fewer stocks than requested (end of data)
            if len(stocks) < limit:
                logger.info("Reached end of available data")
                break
            
            offset += limit
            iteration += 1
            
            # Rate limiting - be respectful
            time.sleep(2)
        
        logger.info(f"Scraping completed. Total stocks found: {len(self.scraped_data)}")
        return self.scraped_data
    
    def save_to_csv(self, filename="tradingview_stocks.csv"):
        """Save scraped data to CSV file"""
        if not self.scraped_data:
            logger.warning("No data to save")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'symbol', 'exchange', 'name', 'description', 'logoid', 'update_mode',
                'type', 'typespecs', 'close', 'pricescale', 'minmov', 'fractional',
                'minmove2', 'currency', 'change', 'volume', 'relative_volume_10d_calc',
                'market_cap_basic', 'free_currency_code', 'price_earnings_ttm',
                'earnings_per_share_diluted_ttm', 'earnings_per_share_diluted_yoy_growth_ttm',
                'dividends_yield_current', 'sector_tr', 'market', 'sector', 'AnalystRating',
                'AnalystRating_tr', 'exchange_field', 'full_symbol'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.scraped_data)
        
        logger.info(f"Data saved to {filename}")
    
    def save_symbol_industry(self, filename="tradingview_symbols.csv"):
        """Save in simplified SYMBOL,INDUSTRY format"""
        if not self.scraped_data:
            logger.warning("No data to save")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Remove duplicates and sort
            seen_symbols = set()
            unique_data = []
            
            for stock in self.scraped_data:
                symbol = stock['symbol']
                industry = stock.get('industry', 'Unknown')
                
                # Use exchange prefix for clarity
                exchange_symbol = f"{stock['exchange']}:{symbol}"
                
                if exchange_symbol not in seen_symbols:
                    unique_data.append([exchange_symbol, industry])
                    seen_symbols.add(exchange_symbol)
            
            # Sort by symbol
            unique_data.sort()
            
            for symbol_exchange, industry in unique_data:
                writer.writerow([symbol_exchange, industry])
        
        logger.info(f"Symbol-Industry data saved to {filename}")

def create_curl_command():
    """Generate curl command example for manual testing"""
    payload = {
        "columns": [
            "name",
            "description",
            "logoid",
            "update_mode",
            "type",
            "typespecs",
            "close",
            "pricescale",
            "minmov",
            "fractional",
            "minmove2",
            "currency",
            "change",
            "volume",
            "relative_volume_10d_calc",
            "market_cap_basic",
            "fundamental_currency_code",
            "price_earnings_ttm",
            "earnings_per_share_diluted_ttm",
            "earnings_per_share_diluted_yoy_growth_ttm",
            "dividends_yield_current",
            "sector.tr",
            "market",
            "sector",
            "AnalystRating",
            "AnalystRating.tr",
            "exchange"
        ],
        "filter": [
            {"left": "exchange", "operation": "in_range", "right": ["BSE"]},
            {"left": "is_primary", "operation": "equal", "right": True}
        ],
        "filter2": {
            "operator": "and",
            "operands": [
                {
                    "operation": {
                        "operator": "or",
                        "operands": [
                            {
                                "operation": {
                                    "operator": "and",
                                    "operands": [
                                        {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                                        {"expression": {"left": "exchange", "operation": "in_range", "right": ["BSE"]}}
                                    ]
                                }
                            },
                            {
                                "operation": {
                                    "operator": "and",
                                    "operands": [
                                        {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                                        {"expression": {"left": "exchange", "operation": "in_range", "right": ["NSE"]}}
                                    ]
                                }
                            },
                            {
                                "operation": {
                                    "operator": "and",
                                    "operands": [
                                        {"expression": {"left": "type", "operation": "equal", "right": "dr"}}
                                    ]
                                }
                            },
                            {
                                "operation": {
                                    "operator": "and",
                                    "operands": [
                                        {"expression": {"left": "type", "operation": "equal", "right": "fund"}},
                                        {"expression": {"left": "exchange", "operation": "in_range", "right": ["BSE"]}}
                                    ]
                                }
                            }
                        ]
                    }
                }
            ]
        },
        "ignore_unknown_fields": False,
        "markets": ["india"],
        "options": {"lang": "en"},
        "range": [0, 100],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "symbols": {}
    }
    
    curl_cmd = f"""curl -X POST 'https://scanner.tradingview.com/india/scan' \\
-H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \\
-H 'Accept: application/json, text/plain, */*' \\
-H 'Accept-Language: en-US,en;q=0.9' \\
-H 'Content-Type: application/json' \\
-H 'Referer: https://www.tradingview.com/' \\
-H 'Origin: https://www.tradingview.com' \\
--data-raw '{json.dumps(payload)}'
"""
    
    with open("/Users/aniketmahato/Desktop/screener scaper/tradingview_curl_example.sh", "w") as f:
        f.write(curl_cmd)
    
    print("Curl command example saved to tradingview_curl_example.sh")
    return curl_cmd

def main():
    """Main function to run the scraper"""
    print("=" * 60)
    print("TradingView India Stock Scanner")
    print("=" * 60)
    print()
    
    # Create curl example
    curl_cmd = create_curl_command()
    print("Curl command example created!")
    print()
    
    # Run scraper
    scanner = TradingViewScanner()
    
    try:
        stocks_data = scanner.scrape_all_stocks(max_iterations=100)  # Up to 10k stocks
        
        if stocks_data:
            # Save detailed data
            scanner.save_to_csv("tradingview_stocks_detailed.csv")
            
            # Save simplified format
            scanner.save_symbol_industry("tradingview_stocks.csv")
            
            print(f"\n" + "=" * 50)
            print("SCANNING COMPLETED SUCCESSFULLY!")
            print("=" * 50)
            print(f"Total stocks found: {len(scanner.scraped_data)}")
            print("\nFiles created:")
            print("- tradingview_stocks_detailed.csv (full data)")
            print("- tradingview_stocks.csv (SYMBOL,INDUSTRY format)")
            print("- tradingview_curl_example.sh (curl command)")
            
            # Show sample data
            print(f"\nSample stocks discovered:")
            print("-" * 40)
            for i, stock in enumerate(scanner.scraped_data[:10]):
                exchange_symbol = f"{stock['exchange']}:{stock['symbol']}"
                industry = stock.get('industry', 'Unknown')[:30]  # Truncate long industry names
                print(f"{exchange_symbol},{industry}")
            
            if len(scanner.scraped_data) > 10:
                print(f"... and {len(scanner.scraped_data) - 10} more")
                
        else:
            print("\nNo data was scraped. Check the logs for errors.")
            
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
