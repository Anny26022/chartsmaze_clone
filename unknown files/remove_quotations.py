#!/usr/bin/env python3
"""
Remove quotations from CSV file
"""

import csv

def main():
    # Read the file and remove quotes
    cleaned_data = []
    
    with open('screener_stocks_by_industry_with_names.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Remove quotes from all field values
            cleaned_row = {}
            for key, value in row.items():
                # Remove quotes from field values
                cleaned_value = value.strip('"').strip("'") if value else value
                cleaned_row[key] = cleaned_value
            
            cleaned_data.append(cleaned_row)
    
    # Write back without aggressive quoting
    with open('screener_stocks_by_industry_with_names.csv', 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['stock_name', 'symbol', 'industry', 'index', 'sector']
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(cleaned_data)
    
    print(f"✅ Successfully removed quotations from CSV file")
    print(f"📊 Processed {len(cleaned_data)} rows")
    print(f"📁 Updated file: screener_stocks_by_industry_with_names.csv")

if __name__ == "__main__":
    main()
