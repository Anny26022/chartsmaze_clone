#!/usr/bin/env python3
"""
Apply all remaining mappings including low confidence ones
"""

import csv

def main():
    # Load all mappings
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
    
    # Count by confidence levels
    high_conf = sum(1 for m in mappings.values() if m['confidence'] > 0.8)
    medium_conf = sum(1 for m in mappings.values() if 0.6 <= m['confidence'] <= 0.8)  
    low_conf = sum(1 for m in mappings.values() if m['confidence'] < 0.6)
    
    print(f"📊 Symbol mapping breakdown:")
    print(f"   High confidence (>0.8): {high_conf}")
    print(f"   Medium confidence (0.6-0.8): {medium_conf}")
    print(f"   Low confidence (<0.6): {low_conf}")
    print(f"   Total mappings: {len(mappings)}")
    
    # Apply ALL mappings
    updated_count = 0
    updated_data = []
    
    print(f"\n🔄 Applying ALL symbol mappings...")
    
    with open('screener_stocks_by_industry_with_names.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            original_symbol = row['symbol']
            
            if original_symbol in mappings:
                mapping = mappings[original_symbol]
                new_symbol = mapping['new_symbol']
                row['symbol'] = new_symbol
                conf = mapping['confidence']
                stock = mapping['stock_name'][:25] + '...' if len(mapping['stock_name']) > 25 else mapping['stock_name']
                
                if conf > 0.8:
                    status = "🟢"
                elif conf > 0.6:
                    status = "🟡"
                else:
                    status = "🔴"
                
                print(f"{status} {stock}: {original_symbol} → {new_symbol} (conf: {conf:.2f})")
                updated_count += 1
            
            updated_data.append(row)
    
    # Write back
    with open('screener_stocks_by_industry_with_names.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['stock_name', 'symbol', 'industry'])
        writer.writeheader()
        writer.writerows(updated_data)
    
    print(f"\n✅ Applied {updated_count} symbol mappings!")
    
    # Check final status
    remaining_numeric = 0
    remaining_symbols = []
    for row in updated_data:
        if row['symbol'].isdigit() and len(row['symbol']) == 6:
            remaining_numeric += 1
            remaining_symbols.append(f"{row['stock_name']}: {row['symbol']}")
    
    print(f"\n📈 Final Status:")
    print(f"   Symbols applied: {updated_count}")
    print(f"   Remaining numeric symbols: {remaining_numeric}")
    
    if remaining_symbols:
        print(f"\n   Remaining numeric symbols:")
        for symbol_info in remaining_symbols:
            print(f"      - {symbol_info}")

if __name__ == "__main__":
    main()
