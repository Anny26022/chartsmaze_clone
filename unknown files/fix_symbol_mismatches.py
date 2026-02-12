#!/usr/bin/env python3
"""
Script to fix obvious symbol mismatches based on company name similarity
"""

import csv
from collections import defaultdict

def load_tradingview_mappings(csv_file):
    """Load TradingView data and create name mappings"""
    name_to_symbol = {}
    symbol_to_name = {}
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row['symbol']
            name = row['name']
            
            # Store both mappings
            name_to_symbol[name] = symbol
            symbol_to_name[symbol] = name
    
    return name_to_symbol, symbol_to_name

def find_best_matches_for_symbol_not_found(screener_data, tradingview_name_to_symbol):
    """Find best matches for symbols that weren't found"""
    
    def normalize_name(name):
        """Normalize company name for better matching"""
        name = name.lower().strip()
        # Remove common prefixes/suffixes
        suffixes = ['ltd', 'limited', 'co', 'company', 'corp', 'corporation', 'inc', 
                   'incorporated', 'pvt', 'private', '&', 'and', 'the']
        for suffix in suffixes:
            if name.endswith(' ' + suffix):
                name = name[:-len(suffix)].strip()
        return name
    
    corrections = []
    
    print("Finding best matches for companies with numeric/unknown symbols...")
    
    for entry in screener_data:
        screener_name = entry['name']
        symbol = entry['symbol']
        line_num = entry['line_num']
        
        # Skip if symbol is already alnanumeric and not numeric
        if symbol.isalnum() and not symbol.isdigit():
            continue
        
        # Skip if symbol is too short
        if len(symbol) < 4:
            continue
        
        # Clean up screener name
        normalized_screener = normalize_name(screener_name)
        
        best_matches = []
        
        # Search through TradingView names
        for tv_name, tv_symbol in tradingview_name_to_symbol.items():
            normalized_tv = normalize_name(tv_name)
            
            # Check for exact name match
            if normalized_screener == normalized_tv:
                best_matches.append((tv_symbol, tv_name, 1.0))
                break
            
            # Check for word overlap
            screener_words = set(normalized_screener.split())
            tv_words = set(normalized_tv.split())
            
            if screener_words and tv_words:
                intersection = screener_words & tv_words
                union = screener_words | tv_words
                
                # Jaccard similarity
                similarity = len(intersection) / len(union) if union else 0
                
                if similarity >= 0.6:  # High threshold for accuracy
                    best_matches.append((tv_symbol, tv_name, similarity))
        
        # Sort by similarity
        best_matches.sort(key=lambda x: x[2], reverse=True)
        
        if best_matches:
            corrections.append({
                'line': line_num,
                'current_name': screener_name,
                'current_symbol': symbol,
                'suggested_symbol': best_matches[0][0],
                'suggested_name': best_matches[0][1],
                'similarity': best_matches[0][2],
                'all_matches': best_matches[:3]
            })
    
    return corrections

def apply_corrections_to_file(file_path, corrections):
    """Apply corrections to the file"""
    
    print(f"\nApplying {len(corrections)} corrections...")
    
    # Read the file
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Create mapping of line number to new symbol
    line_changes = {}
    for correction in corrections:
        line_changes[correction['line']] = correction['suggested_symbol']
    
    # Apply changes
    changes_made = 0
    for i, line in enumerate(lines):
        line_num = i + 1
        if line_num in line_changes:
            parts = line.strip().split(',')
            if len(parts) >= 3:
                old_symbol = parts[1]
                new_symbol = line_changes[line_num]
                parts[1] = new_symbol
                lines[i] = ','.join(parts) + '\n'
                changes_made += 1
                print(f"Line {line_num}: {parts[0]} → {old_symbol} to {new_symbol}")
    
    # Write updated file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    return changes_made

def manual_corrections():
    """Apply specific manual corrections based on my analysis"""
    manual_fixes = [
        # (line_num, old_symbol, new_symbol, reason)
        (3, '538970', 'WARDINMOBI', 'Wardwizard Innovations & Mobility Ltd.'),
        (4, 'MERCURYEV', 'SEVL', 'Supertech EV Ltd (not Mercury Ev-Tech)'),
    ]
    return manual_fixes

def apply_manual_corrections(file_path, manual_fixes):
    """Apply manual corrections"""
    print(f"\nApplying {len(manual_fixes)} manual corrections...")
    
    # Read the file
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    changes_made = 0
    for line_num, old_symbol, new_symbol, reason in manual_fixes:
        if line_num <= len(lines):
            line = lines[line_num - 1]
            parts = line.strip().split(',')
            if len(parts) >= 3 and parts[1] == old_symbol:
                parts[1] = new_symbol
                lines[line_num - 1] = ','.join(parts) + '\n'
                changes_made += 1
                print(f"Line {line_num}: {parts[0]} → {old_symbol} to {new_symbol} ({reason})")
    
    # Write updated file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    return changes_made

def main():
    # File paths
    tradingview_file = '/Users/aniketmahato/Desktop/screener scaper/tradingview_stocks_detailed.csv'
    screener_file = '/Users/aniketmahato/Desktop/screener scaper/screener_stocks_by_industry_with_names.txt'
    
    print("Loading TradingView mappings...")
    name_to_symbol, symbol_to_name = load_tradingview_mappings(tradingview_file)
    print(f"Loaded {len(name_to_symbol)} TradingView entries")
    
    print("\nLoading screener data...")
    screener_data = []
    with open(screener_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        for line_num, row in enumerate(reader, start=2):
            if len(row) >= 3:
                screener_data.append({
                    'line_num': line_num,
                    'name': row[0].strip(),
                    'symbol': row[1].strip(),
                    'industry': row[2].strip()
                })
    
    print(f"Loaded {len(screener_data)} screener entries")
    
    # Find corrections for companies with numeric/unknown symbols
    corrections = find_best_matches_for_symbol_not_found(screener_data, name_to_symbol)
    
    # Apply manual corrections first
    manual_fixes = manual_corrections()
    manual_changes = apply_manual_corrections(screener_file, manual_fixes)
    
    # Apply automated corrections for high-confidence matches only
    high_confidence_corrections = [c for c in corrections if c['similarity'] >= 0.8]
    
    if high_confidence_corrections:
        print(f"\nFound {len(high_confidence_corrections)} high-confidence matches:")
        for correction in high_confidence_corrections[:10]:  # Show first 10
            print(f"  {correction['current_name']} ({correction['current_symbol']}) → {correction['suggested_symbol']} ({correction['similarity']:.2f})")
        
        if len(high_confidence_corrections) > 10:
            print(f"  ... and {len(high_confidence_corrections) - 10} more")
        
        auto_changes = apply_corrections_to_file(screener_file, high_confidence_corrections)
    else:
        auto_changes = 0
        print("\nNo high-confidence automatic matches found")
    
    print(f"\n📊 SUMMARY:")
    print(f"   Manual corrections applied: {manual_changes}")
    print(f"   Automatic corrections applied: {auto_changes}")
    print(f"   Total changes made: {manual_changes + auto_changes}")

if __name__ == "__main__":
    main()
