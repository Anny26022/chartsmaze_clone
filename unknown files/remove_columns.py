#!/usr/bin/env python3
"""
Remove index and sector columns from screener_stocks_by_industry_with_names.csv
"""

import csv

def main():
    # Columns to keep
    columns_to_keep = ['stock_name', 'symbol', 'industry', 'basic_industry']
    
    # Read the current file
    cleaned_data = []
    
    with open('screener_stocks_by_industry_with_names.csv', 'r') as infile:
        reader = csv.DictReader(infile)
        
        # Process each row
        for row in reader:
            # Create new row with only desired columns
            cleaned_row = {col: row.get(col, '') for col in columns_to_keep}
            cleaned_data.append(cleaned_row)
    
    # Write the cleaned data back
    with open('screener_stocks_by_industry_with_names.csv', 'w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=columns_to_keep)
        writer.writeheader()
        writer.writerows(cleaned_data)
    
    print(f"✅ Successfully removed columns: index, sector")
    print(f"📊 Kept columns: {', '.join(columns_to_keep)}")
    print(f"📈 Total rows processed: {len(cleaned_data)}")
    print(f"📁 Updated file: screener_stocks_by_industry_with_names.csv")

if __name__ == "__main__":
    main()
