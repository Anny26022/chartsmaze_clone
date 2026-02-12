#!/usr/bin/env python3
"""
Match symbols between screener_stocks_by_industry_with_names.csv and Basic RS Setup.csv
Fetch index and sector information from Basic RS Setup.csv and add to screener file
"""

import csv
import difflib
import re

def normalize_symbol(symbol):
    """Normalize symbol for comparison - remove special chars, convert to uppercase"""
    if isinstance(symbol, str):
        # Remove special characters and convert to uppercase
        normalized = re.sub(r'[^A-Z0-9]', '', symbol.upper())
        return normalized
    return str(symbol)

def load_basic_rs_data():
    """Load Basic RS Setup data"""
    basic_rs_data = {}
    
    try:
        with open('Basic RS Setup.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stock_name = row.get('Stock Name', '').strip()
                basic_industry = row.get('Basic Industry', '').strip()
                index = row.get('Index', '').strip()
                sector = row.get('Sector', '').strip()
                
                if stock_name:
                    basic_rs_data[stock_name] = {
                        'basic_industry': basic_industry,
                        'index': index,
                        'sector': sector
                    }
        
        print(f"Loaded {len(basic_rs_data)} entries from Basic RS Setup.csv")
        return basic_rs_data
        
    except Exception as e:
        print(f"Error loading Basic RS Setup data: {e}")
        return {}

def find_best_match(symbol, basic_rs_data):
    """Find the best match for a symbol in Basic RS data"""
    if not symbol:
        return None
    
    normalized_input = normalize_symbol(symbol)
    
    # Exact match first
    if normalized_input in basic_rs_data:
        return {
            'matched_symbol': symbol,
            'similarity': 1.0,
            'match_type': 'exact'
        }
    
    # Fuzzy matching
    best_match = None
    best_similarity = 0.0
    threshold = 0.95  # 95% similarity threshold
    
    for rs_symbol in basic_rs_data.keys():
        normalized_rs = normalize_symbol(rs_symbol)
        
        # Check normalized match
        if normalized_input == normalized_rs:
            return {
                'matched_symbol': rs_symbol,
                'similarity': 1.0,
                'match_type': 'normalized_exact'
            }
        
        # Fuzzy string matching
        similarity = difflib.SequenceMatcher(None, normalized_input, normalized_rs).ratio()
        
        if similarity > best_similarity and similarity >= threshold:
            best_similarity = similarity
            best_match = rs_symbol
    
    if best_match:
        return {
            'matched_symbol': best_match,
            'similarity': best_similarity,
            'match_type': 'fuzzy'
        }
    
    return None

def main():
    # Load Basic RS Setup data
    basic_rs_data = load_basic_rs_data()
    if not basic_rs_data:
        print("No Basic RS Setup data loaded!")
        return
    
    # Process screener file
    updated_data = []
    matched_count = 0
    exact_matches = 0
    normalized_matches = 0
    fuzzy_matches = 0
    
    print("\nProcessing screener file and matching symbols...")
    
    with open('screener_stocks_by_industry_with_names.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            symbol = row.get('symbol', '').strip()
            stock_name = row.get('stock_name', '').strip()
            
            # Initialize with original data
            updated_row = row.copy()
            updated_row['index'] = ''
            updated_row['sector'] = ''
            
            # Find match
            match_result = find_best_match(symbol, basic_rs_data)
            
            if match_result:
                matched_symbol = match_result['matched_symbol']
                match_type = match_result['match_type']
                similarity = match_result['similarity']
                
                if matched_symbol in basic_rs_data:
                    matched_data = basic_rs_data[matched_symbol]
                    updated_row['index'] = matched_data['index']
                    updated_row['sector'] = matched_data['sector']
                    
                    matched_count += 1
                    
                    if match_type == 'exact':
                        exact_matches += 1
                    elif match_type == 'normalized_exact':
                        normalized_matches += 1
                    elif match_type == 'fuzzy':
                        fuzzy_matches += 1
                    
                    # Show some matches for verification
                    if matched_count <= 10:
                        print(f"  ✓ {symbol} → {matched_symbol} ({similarity:.1%}) [{match_type}] - Sector: {matched_data['sector']}")
            
            updated_data.append(updated_row)
    
    # Write updated file
    with open('screener_stocks_by_industry_with_names.csv', 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['stock_name', 'symbol', 'industry', 'index', 'sector']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(updated_data)
    
    print(f"\n📊 Matching Results:")
    print(f"  Total stocks processed: {len(updated_data)}")
    print(f"  Symbols matched: {matched_count}")
    print(f"    - Exact matches: {exact_matches}")
    print(f"    - Normalized matches: {normalized_matches}")
    print(f"    - Fuzzy matches (95%+): {fuzzy_matches}")
    print(f"  Success rate: {(matched_count/len(updated_data))*100:.1f}%")
    print(f"\n✅ Updated screener_stocks_by_industry_with_names.csv with index and sector data!")

if __name__ == "__main__":
    main()
