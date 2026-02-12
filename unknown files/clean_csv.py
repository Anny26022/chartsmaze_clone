#!/usr/bin/env python3
"""
Script to clean screener_stocks_by_industry_with_names.csv by:
1. Removing the stock_name field
2. Removing unnecessary quotations
3. Creating a new cleaned file
"""

import csv
import re

def clean_csv_file(input_file, output_file):
    """Clean the CSV file by removing stock_name field and unnecessary quotes."""
    
    cleaned_rows = []
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            # Get fieldnames and remove stock_name
            fieldnames = [field for field in reader.fieldnames if field != 'stock_name']
            
            for row in reader:
                cleaned_row = {}
                for field in fieldnames:
                    # Remove unnecessary quotes and clean the value
                    value = str(row[field]).strip()
                    
                    # Remove surrounding quotes if they exist
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    if value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # Remove any remaining unnecessary quotes
                    value = value.replace('""', '"').replace("''", "'")
                    
                    cleaned_row[field] = value
                
                cleaned_rows.append(cleaned_row)
        
        # Write cleaned data to new file
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_rows)
        
        print(f"✅ Successfully cleaned CSV file!")
        print(f"📁 Input: {input_file}")
        print(f"📁 Output: {output_file}")
        print(f"📊 Total rows: {len(cleaned_rows)}")
        print(f"🗑️ Removed field: stock_name")
        print(f"🧹 Cleaned quotations")
        
        # Show sample of cleaned data
        print(f"\n📋 Sample of cleaned data:")
        print(f"Fields: {', '.join(fieldnames)}")
        if cleaned_rows:
            sample_row = cleaned_rows[0]
            for field, value in sample_row.items():
                print(f"  {field}: {value}")
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")

if __name__ == "__main__":
    input_file = "screener_stocks_by_industry_with_names.csv"
    output_file = "screener_stocks_cleaned.csv"
    
    clean_csv_file(input_file, output_file)
