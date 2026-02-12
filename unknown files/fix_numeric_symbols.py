#!/usr/bin/env python3
"""
Simple script to replace numeric symbols with proper symbols
"""

import csv
import sys

def load_mappings():
    """Load mappings from symbol_matches_found.csv"""
    mappings = {}
    
    try:
        with open('symbol_matches_found.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip empty rows
                if not row or not any(row.values()):
                    continue
                    
                old_symbol = row.get('old_symbol', '').strip()
                new_symbol = row.get('new_symbol', '').strip()
                
                if old_symbol and new_symbol:
                    mappings[old_symbol] = new_symbol
        
        print(f"Loaded {len(mappings)} symbol mappings")
        return mappings
        
    except Exception as e:
        print(f"Error loading mappings: {e}")
        return {}

def update_screener_file():
    """Update the screener file with new symbols"""
    
    mappings = load_mappings()
    if not mappings:
        print("No mappings loaded!")
        return
    
    # Read current file
    updated_data = []
    updated_count = 0
    
    with open('screener_stocks_by_industry_with_names.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            original_symbol = row['symbol']
            
            # Check if this symbol has a mapping
            if original_symbol in mappings:
                new_symbol = mappings[original_symbol]
                row['symbol'] = new_symbol
                updated_count += 1
                print(f"Updated: {original_symbol} → {new_symbol}")
            
            updated_data.append(row)
    
    # Write back to file
    with open('screener_stocks_by_industry_with_names.csv', 'w', newline='') as f:
        fieldnames = ['stock_name', 'symbol', 'industry']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_data)
    
    print(f"\n✅ Successfully updated {updated_count} symbols!")
    print(f"📁 File updated: screener_stocks_by_industry_with_names.csv")

if __name__ == "__main__":
    update_screener_file()
