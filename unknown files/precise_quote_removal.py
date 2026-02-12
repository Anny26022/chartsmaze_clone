#!/usr/bin/env python3
"""
Precise removal of quotations from CSV using regex
"""

import re

def main():
    # Read the file
    with open('screener_stocks_by_industry_with_names.csv', 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("Original content sample:")
    print(content[:500])
    print("\n" + "="*50 + "\n")
    
    # Remove quotes around comma-separated values
    # Pattern: "text,text,text" becomes text,text,text
    pattern = r'"[^"]*(,[^"]*)*"'
    
    def remove_quotes_from_matches(match):
        quoted_content = match.group(0)
        # Remove the outer quotes
        unquoted = quoted_content[1:-1]
        return unquoted
    
    cleaned_content = re.sub(pattern, remove_quotes_from_matches, content)
    
    print("Cleaned content sample:")
    print(cleaned_content[:500])
    
    # Write back
    with open('screener_stocks_by_industry_with_names.csv', 'w', encoding='utf-8') as f:
        f.write(cleaned_content)
    
    print(f"\n✅ Removed quotations from CSV file")
    print(f"📊 File updated successfully")

if __name__ == "__main__":
    main()
