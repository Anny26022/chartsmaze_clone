#!/usr/bin/env python3
"""
Script to add missing TradingView data to screener file
"""

import csv
import sys

def read_screener_data(filename):
    """Read screener data into a set for quick lookup"""
    screener_symbols = set()
    screener_data = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            screener_symbols.add(row['symbol'])
            screener_data.append(row)
    
    return screener_symbols, screener_data

def read_tradingview_data(filename):
    """Read TradingView data"""
    tradingview_data = []
    
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tradingview_data.append(row)
    
    return tradingview_data

def find_missing_companies(screener_symbols, tradingview_data):
    """Find TradingView companies that are missing from screener"""
    missing_companies = []
    
    print("Analyzing TradingView companies missing from screener...")
    
    for tv_company in tradingview_data:
        symbol = tv_company['symbol']
        name = tv_company['name']
        sector = tv_company.get('sector', 'Unknown')
        market = tv_company.get('market', 'India')
        
        if symbol not in screener_symbols:
            missing_companies.append({
                'symbol': symbol,
                'name': name,
                'industry': sector,
                'description': f"{name} ({symbol})",
                'market': market
            })
    
    return missing_companies

def add_missing_to_screener(screener_data, missing_companies, output_filename):
    """Add missing companies to screener data"""
    
    # Add missing companies to the data
    for company in missing_companies:
        screener_data.append({
            'stock_name': company['name'],
            'symbol': company['symbol'],
            'industry': company['industry']
        })
    
    # Write the updated data
    with open(output_filename, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['stock_name', 'symbol', 'industry']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in screener_data:
            writer.writerow(row)
    
    return len(screener_data)

def main():
    screener_file = 'screener_stocks_by_industry_with_names.csv'
    tradingview_file = 'tradingview_stocks_detailed.csv'
    output_file = 'screener_stocks_complete.csv'
    
    print("Reading screener data...")
    screener_symbols, screener_data = read_screener_data(screener_file)
    print(f"Found {len(screener_data)} companies in screener file")
    
    print("Reading TradingView data...")
    tradingview_data = read_tradingview_data(tradingview_file)
    print(f"Found {len(tradingview_data)} companies in TradingView file")
    
    print("Finding missing companies...")
    missing_companies = find_missing_companies(screener_symbols, tradingview_data)
    print(f"Found {len(missing_companies)} TradingView companies missing from screener")
    
    if missing_companies:
        print("\nFirst 10 missing companies:")
        for i, company in enumerate(missing_companies[:10]):
            print(f"{i+1}. {company['symbol']} - {company['name']} ({company['industry']})")
            
        print(f"\nAdding {len(missing_companies)} missing companies to screener file...")
        total_companies = add_missing_to_screener(screener_data, missing_companies, output_file)
        
        print(f"✅ Complete screener file created: {output_file}")
        print(f"📊 Total companies: {total_companies}")
        print(f"➕ Added companies: {len(missing_companies)}")
    else:
        print("🎉 All TradingView companies are already in the screener file!")

if __name__ == "__main__":
    main()
