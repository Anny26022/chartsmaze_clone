#!/usr/bin/env python3
"""
Final script to extract company IDs from Screener.in for all companies.
This script successfully extracts data-row-company-id values from Screener pages.

Usage:
    python3 run_company_id_extraction.py          # Test with 50 companies
    python3 run_company_id_extraction.py --all    # Process all companies
"""

import csv
import requests
import time
import re
import random
import urllib3
from bs4 import BeautifulSoup
import sys

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def extract_company_id_from_page(symbol, max_retries=3):
    """Extract company ID from Screener company page source."""
    for attempt in range(max_retries):
        try:
            url = f"https://www.screener.in/company/{symbol}/consolidated/"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Cache-Control': 'no-cache',
            }
            
            response = requests.get(url, headers=headers, timeout=20, verify=False)
            
            # Handle different response codes
            if response.status_code == 200:
                # Check if we got blocked (common blocked content)
                if "blocked" in response.text.lower() or "rate limit" in response.text.lower() or len(response.text) < 1000:
                    print(f"    ⚠️ Possible blocking detected, attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(10, 20))  # Longer delay if blocked
                        continue
                    else:
                        return None
                
                # Extract company ID from data-row-company-id attribute
                pattern = r'data-row-company-id=["\'](\d+)["\']'
                matches = re.findall(pattern, response.text)
                
                if matches:
                    return matches[0]
                
                # Fallback: extract from JavaScript/JSON
                soup = BeautifulSoup(response.text, 'html.parser')
                scripts = soup.find_all('script')
                
                for script in scripts:
                    script_text = script.string or ""
                    # Look for company ID patterns in JavaScript
                    patterns = [
                        r'"companyId["\']?\s*:\s*["\']?(\d+)["\']?',
                        r'"id["\']?\s*:\s*["\']?(\d+)["\']?\s*[,}]',
                        r'company_id["\']?\s*[:=]\s*["\']?(\d+)["\']?',
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, script_text)
                        if matches:
                            for match in matches:
                                if len(match) >= 4:  # Company IDs are typically longer
                                    return match
                
                # Fallback: look for any 6+ digit numbers (heuristic)
                long_numbers = re.findall(r'\b(\d{6,})\b', response.text)
                for num in long_numbers:
                    num_int = int(num)
                    if 100000 <= num_int <= 9999999:  # Reasonable range for company IDs
                        return num
                
                return None
                
            elif response.status_code == 429:  # Rate limited
                print(f"    ⚠️ Rate limited, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(15, 30))
                    continue
                else:
                    return None
                    
            elif response.status_code == 403:  # Forbidden/blocked
                print(f"    ⚠️ Access forbidden, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(20, 40))
                    continue
                else:
                    return None
                    
            elif response.status_code == 404:
                return None  # Company not found on Screener
                
            else:
                print(f"    ⚠️ HTTP {response.status_code}, attempt {attempt + 1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(5, 10))
                    continue
                else:
                    return None
        
        except Exception as e:
            print(f"    Error (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))
                continue
            else:
                return None
    
    return None

def process_companies(max_companies=None):
    """Process companies from the CSV file and extract company IDs."""
    
    # Read companies from CSV
    companies = []
    try:
        with open('screener_stocks_by_industry_with_names.csv', 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                symbol = row['symbol'].strip()
                # Skip numeric symbols
                if not symbol.isdigit():
                    companies.append({
                        'stock_name': row['stock_name'].strip(),
                        'symbol': symbol,
                        'industry': row['industry'].strip()
                    })
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return
    
    if max_companies:
        companies = companies[:max_companies]
    
    print(f"Processing {len(companies)} companies...")
    
    results = []
    failed = []
    
    for i, company in enumerate(companies):
        print(f"{i+1:4d}/{len(companies)} | {company['symbol']:12} | {company['stock_name']}")
        
        # Add longer delay to avoid being blocked by Screener
        time.sleep(random.uniform(3, 6))
        
        company_id = extract_company_id_from_page(company['symbol'])
        
        if company_id:
            results.append({
                'stock_name': company['stock_name'],
                'symbol': company['symbol'],
                'industry': company['industry'],
                'company_id': company_id,
                'screener_url': f"https://www.screener.in/company/{company['symbol']}/consolidated/",
                'status': 'success'
            })
            print(f"    ✓ {company_id}")
        else:
            failed.append({
                'stock_name': company['stock_name'],
                'symbol': company['symbol'],
                'industry': company['industry'],
                'company_id': '',
                'screener_url': f"https://www.screener.in/company/{company['symbol']}/consolidated/",
                'status': 'failed'
            })
            print(f"    ✗ Not found")
        
        # Show progress every 25 companies
        if (i + 1) % 25 == 0:
            current_total = len(results) + len(failed)
            progress = (current_total / len(companies)) * 100 if len(companies) > 0 else 0
            print(f"    Progress: {len(results)} successful, {len(failed)} failed ({progress:.1f}% complete)...")
    
    # Save final results to a single file
    all_results = results + failed
    save_results(all_results, "all_companies_with_ids.csv")
    
    # Print summary
    total = len(all_results)
    success_count = len(results)
    success_rate = (success_count / total * 100) if total > 0 else 0
    
    print(f"\n=== EXTRACTION COMPLETE ===")
    print(f"Total processed: {total}")
    print(f"Successful: {success_count}")
    print(f"Failed: {len(failed)}")
    print(f"Success rate: {success_rate:.1f}%")
    
    print(f"\nSample successful extractions:")
    for result in results[:10]:
        print(f"  {result['symbol']:12} -> {result['company_id']}")

def save_results(results, filename):
    """Save results to CSV file."""
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            fieldnames = ['stock_name', 'symbol', 'industry', 'company_id', 'screener_url', 'status']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    except Exception as e:
        print(f"Error saving {filename}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        print("🧪 Starting extraction TEST with 50 companies...")
        print("Run without --test to process all companies.\n")
        process_companies(max_companies=50)
    else:
        print("🚀 Starting extraction for ALL companies...")
        print("⚠️  This will take several hours due to rate limiting.")
        print("Ctrl+C to cancel if needed.\n")
        
        try:
            process_companies()
        except KeyboardInterrupt:
            print("\n⏹️  Extraction cancelled by user.")
