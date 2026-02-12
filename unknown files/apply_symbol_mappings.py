#!/usr/bin/env python3
"""
Script to apply symbol mappings from symbol_matches_found.csv to the main screener file
"""

import csv
import sys

def load_symbol_mappings(mappings_file):
    """Load symbol mappings from symbol_matches_found.csv"""
    mappings = {}
    
    print("Loading symbol mappings...")
    with open(mappings_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row and row.get('old_symbol') and row.get('new_symbol'):
                old_symbol = row['old_symbol']
                new_symbol = row['new_symbol']
                stock_name = row.get('stock_name', '')
                mappings[old_symbol] = {
                    'new_symbol': new_symbol,
                    'stock_name': stock_name,
                    'confidence': float(row.get('confidence', 0)),
                    'tv_name': row.get('tv_name', '')
                }
    
    print(f"Loaded {len(mappings)} symbol mappings")
    return mappings

def apply_mappings(screener_file, mappings, output_file):
    """Apply symbol mappings to screener file"""
    
    updated_count = 0
    total_lines = 0
    
    with open(screener_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            total_lines += 1
            old_symbol = row['symbol']
            
            # Check if this symbol has a mapping
            if old_symbol in mappings and mappings[old_symbol]['confidence'] > 0.7:
                # Apply the mapping
                new_symbol = mappings[old_symbol]['new_symbol']
                row['symbol'] = new_symbol
                updated_count += 1
                print(f"✅ {mappings[old_symbol]['stock_name']}: {old_symbol} → {new_symbol} (confidence: {mappings[old_symbol]['confidence']:.2f})")
            
            writer.writerow(row)
    
    print(f"\n📊 Results:")
    print(f"Total lines processed: {total_lines}")
    print(f"Symbols updated: {updated_count}")
    print(f"Success rate: {(updated_count/total_lines)*100:.1f}%")
    
    return updated_count

def main():
    mappings_file = 'symbol_matches_found.csv'
    screener_file = 'screener_stocks_by_industry_with_names.csv'
    output_file = 'screener_stocks_by_industry_with_names.csv'
    
    try:
        # Load mappings
        mappings = load_symbol_mappings(mappings_file)
        
        if not mappings:
            print("❌ No mappings loaded. Check symbol_matches_found.csv")
            return
        
        # Apply mappings
        print(f"\nApplying mappings to {screener_file}...")
        updated_count = apply_mappings(screener_file, mappings, output_file)
        
        print(f"\n✅ Successfully updated {updated_count} symbols!")
        print(f"📁 Updated file: {output_file}")
        
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
