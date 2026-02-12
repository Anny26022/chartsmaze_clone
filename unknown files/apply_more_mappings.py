#!/usr/bin/env python3
"""
Apply medium+high confidence symbol mappings (>0.6 confidence)
"""

import csv
import sys

def load_mappings():
    """Load mappings from symbol_matches_found.csv"""
    mappings = {}
    
    with open('symbol_matches_found.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row or not any(row.values()):
                continue
                
            old_symbol = row.get('old_symbol', '').strip()
            new_symbol = row.get('new_symbol', '').strip()
            confidence = float(row.get('confidence', 0))
            stock_name = row.get('stock_name', '').strip()
            
            if old_symbol and new_symbol:
                mappings[old_symbol] = {
                    'new_symbol': new_symbol,
                    'confidence': confidence,
                    'stock_name': stock_name
                }
    
    return mappings

def main():
    mappings = load_mappings()
    print(f"Loaded {len(mappings)} total mappings")
    
    # Apply mappings with confidence > 0.6 (medium + high)
    threshold = 0.6
    medium_high_mappings = {k: v for k, v in mappings.items() if v['confidence'] > threshold}
    print(f"Applying {len(medium_high_mappings)} medium+high confidence mappings (>{threshold})")
    
    updated_count = 0
    updated_data = []
    
    with open('screener_stocks_by_industry_with_names.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            original_symbol = row['symbol']
            
            # Apply mapping if confidence > 0.6
            if original_symbol in medium_high_mappings:
                mapping = medium_high_mappings[original_symbol]
                new_symbol = mapping['new_symbol']
                row['symbol'] = new_symbol
                conf = mapping['confidence']
                stock = mapping['stock_name'][:30] + '...' if len(mapping['stock_name']) > 30 else mapping['stock_name']
                print(f"✓ {stock}: {original_symbol} → {new_symbol} (conf: {conf:.2f})")
                updated_count += 1
            
            updated_data.append(row)
    
    # Write back
    with open('screener_stocks_by_industry_with_names.csv', 'w', newline='') as f:
        fieldnames = ['stock_name', 'symbol', 'industry']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_data)
    
    print(f"\n✅ Applied {updated_count} additional symbol mappings!")
    
    # Count remaining numeric symbols
    remaining = 0
    for row in updated_data:
        if row['symbol'].isdigit() and len(row['symbol']) == 6:
            remaining += 1
            if remaining <= 5:  # Show first 5
                print(f"   Remaining: {row['stock_name']} -> {row['symbol']}")
    
    print(f"📊 Remaining numeric symbols: {remaining}")

if __name__ == "__main__":
    main()
