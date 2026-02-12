#!/usr/bin/env python3
"""
Script to verify all symbols in screener file against TradingView data
"""

import csv
from collections import defaultdict

def load_tradingview_data(csv_file):
    """Load all TradingView data"""
    tradingview_data = {}
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = row['symbol']
            name = row['name']
            exchanges = row.get('exchange', '').strip()
            
            # Store data indexed by symbol
            tradingview_data[symbol] = {
                'symbol': symbol,
                'name': name,
                'exchange': exchanges
            }
    
    return tradingview_data

def load_screener_file(txt_file):
    """Load screener file data"""
    screener_data = []
    
    with open(txt_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 because header counts as line 1
            if len(row) >= 3:
                name = row[0].strip()
                symbol = row[1].strip()
                industry = row[2].strip()
                
                screener_data.append({
                    'line_num': row_num,
                    'name': name,
                    'symbol': symbol,
                    'industry': industry
                })
    
    return screener_data

def verify_symbols(screener_data, tradingview_data):
    """Verify each symbol in screener data"""
    verification_results = {
        'correct': [],
        'incorrect_symbol': [],
        'symbol_not_found': [],
        'partial_matches': []
    }
    
    print("Starting comprehensive symbol verification...\n")
    
    for entry in screener_data:
        name = entry['name']
        symbol = entry['symbol']
        industry = entry['industry']
        line_num = entry['line_num']
        
        # Check if symbol exists in TradingView data
        if symbol in tradingview_data:
            tv_entry = tradingview_data[symbol]
            tv_name = tv_entry['name']
            
            # Check if the names match (case-insensitive)
            if name.lower() == tv_name.lower():
                verification_results['correct'].append({
                    'line': line_num,
                    'screener_name': name,
                    'symbol': symbol,
                    'tv_name': tv_name,
                    'industry': industry
                })
            else:
                # Symbol exists in TradingView but names don't match
                verification_results['incorrect_symbol'].append({
                    'line': line_num,
                    'screener_name': name,
                    'symbol': symbol,
                    'tv_name': tv_name,
                    'industry': industry
                })
        else:
            # Symbol not found in TradingView
            verification_results['symbol_not_found'].append({
                'line': line_num,
                'screener_name': name,
                'symbol': symbol,
                'industry': industry
            })
    
    return verification_results

def find_better_matches(symbol_not_found, tradingview_data):
    """Find better symbol matches for companies with missing symbols"""
    suggestions = []
    
    print("\nSearching for better matches...\n")
    
    for entry in symbol_not_found:
        screener_name = entry['screener_name']
        current_symbol = entry['symbol']
        industry = entry['industry']
        line_num = entry['line']
        
        # Search for name matches in TradingView data
        matches = []
        
        for tv_symbol, tv_data in tradingview_data.items():
            tv_name = tv_data['name']
            
            # Simple fuzzy matching - check if key words match
            screener_words = set(screener_name.lower().split())
            tv_words = set(tv_name.lower().split())
            
            # Remove common words
            common_words = {'ltd', 'limited', 'co', 'company', 'corp', 'corporation', 'inc', 
                          'incorporated', 'pvt', 'private', '&', 'and', 'the', 'of', 'in'}
            screener_words -= common_words
            tv_words -= common_words
            
            # Calculate similarity
            if screener_words and tv_words:
                intersection = screener_words & tv_words
                similarity = len(intersection) / min(len(screener_words), len(tv_words))
                
                if similarity >= 0.5:  # At least 50% word overlap
                    matches.append({
                        'symbol': tv_symbol,
                        'name': tv_name,
                        'similarity': similarity
                    })
        
        # Sort matches by similarity
        matches.sort(key=lambda x: x['similarity'], reverse=True)
        
        if matches:
            suggestions.append({
                'line': line_num,
                'current_symbol': current_symbol,
                'screener_name': screener_name,
                'industry': industry,
                'matches': matches[:3]  # Top 3 matches
            })
    
    return suggestions

def print_verification_report(results):
    """Print detailed verification report"""
    
    print("=" * 80)
    print("SYMBOL VERIFICATION REPORT")
    print("=" * 80)
    
    print(f"\n✅ CORRECT SYMBOLS ({len(results['correct'])}):")
    print("-" * 50)
    for entry in results['correct'][:10]:  # Show first 10
        print(f"Line {entry['line']}: {entry['screener_name']} → {entry['symbol']}")
    if len(results['correct']) > 10:
        print(f"... and {len(results['correct']) - 10} more correct symbols")
    
    print(f"\n❌ INCORRECT SYMBOLS ({len(results['incorrect_symbol'])}):")
    print("-" * 10)
    for entry in results['incorrect_symbol']:
        print(f"Line {entry['line']}: '{entry['screener_name']}' → '{entry['symbol']}'")
        print(f"   TradingView shows: '{entry['tv_name']}' → '{entry['symbol']}'")
        print()
    
    print(f"\n🔍 SYMBOLS NOT FOUND ({len(results['symbol_not_found'])}):")
    print("-" * 10)
    for entry in results['symbol_not_found'][:20]:  # Show first 20
        print(f"Line {entry['line']}: {entry['screener_name']} → {entry['symbol']}")
    if len(results['symbol_not_found']) > 20:
        print(f"... and {len(results['symbol_not_found']) - 20} more not found")
    
    return len(results['incorrect_symbol'])

def print_suggestions(suggestions):
    """Print improvement suggestions"""
    if not suggestions:
        return
    
    print(f"\n💡 IMPROVEMENT SUGGESTIONS ({len(suggestions)}):")
    print("-" * 50)
    
    for suggestion in suggestions[:15]:  # Show first 15
        print(f"\nLine {suggestion['line']}: {suggestion['screener_name']}")
        print(f"  Current symbol: {suggestion['current_symbol']}")
        print("  Possible matches:")
        
        for i, match in enumerate(suggestion['matches'], 1):
            print(f"    {i}. {match['symbol']} - '{match['name']}' (similarity: {match['similarity']:.2f})")
    
    if len(suggestions) > 15:
        print(f"\n... and {len(suggestions) - 15} more suggestions")

def create_correction_file(results, suggestions):
    """Create a file with suggested corrections"""
    corrections = []
    
    # Add incorrect symbols that should be fixed
    for entry in results['incorrect_symbol']:
        corrections.append({
            'line': entry['line'],
            'action': 'fix_name_mismatch',
            'current_name': entry['screener_name'],
            'current_symbol': entry['symbol'],
            'tradingview_name': entry['tv_name'],
            'suggestion': f"Review: '{entry['screener_name']}' vs '{entry['tv_name']}' for symbol '{entry['symbol']}'"
        })
    
    # Add symbols to potentially replace
    for suggestion in suggestions:
        if suggestion['matches']:
            best_match = suggestion['matches'][0]
            corrections.append({
                'line': suggestion['line'],
                'action': 'replace_symbol',
                'current_name': suggestion['screener_name'],
                'current_symbol': suggestion['current_symbol'],
                'suggested_symbol': best_match['symbol'],
                'suggested_name': best_match['name'],
                'suggestion': f"Replace '{suggestion['current_symbol']}' with '{best_match['symbol']}'"
            })
    
    return corrections

def main():
    # File paths
    import os
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    tradingview_file = os.path.join(BASE_DIR, 'tradingview_stocks_detailed.csv')
    screener_file = os.path.join(BASE_DIR, 'screener_stocks_by_industry_with_names.txt')
    
    print("Loading TradingView data...")
    tradingview_data = load_tradingview_data(tradingview_file)
    print(f"Loaded {len(tradingview_data)} TradingView entries")
    
    print("\nLoading screener data...")
    screener_data = load_screener_file(screener_file)
    print(f"Loaded {len(screener_data)} screener entries")
    
    print("\nVerifying symbols...")
    results = verify_symbols(screener_data, tradingview_data)
    
    print("\nFinding better matches...")
    suggestions = find_better_matches(results['symbol_not_found'], tradingview_data)
    
    print_verification_report(results)
    print_suggestions(suggestions)
    
    print(f"\n📊 SUMMARY:")
    print(f"   Total entries: {len(screener_data)}")
    print(f"   Correct symbols: {len(results['correct'])} ({len(results['correct'])/len(screener_data)*100:.1f}%)")
    print(f"   Incorrect symbols: {len(results['incorrect_symbol'])} ({len(results['incorrect_symbol'])/len(screener_data)*100:.1f}%)")
    print(f"   Not found: {len(results['symbol_not_found'])} ({len(results['symbol_not_found'])/len(screener_data)*100:.1f}%)")
    
    # Create corrections file
    corrections = create_correction_file(results, suggestions)
    if corrections:
        print(f"\n📝 Generated {len(corrections)} correction suggestions")
        print("You may want to review and apply these corrections manually")

if __name__ == "__main__":
    main()
