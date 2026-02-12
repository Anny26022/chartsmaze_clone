#!/usr/bin/env python3
"""
Manual removal of quotations from CSV
"""

def main():
    # Read the file as raw text and remove quotes manually
    with open('screener_stocks_by_industry_with_names.csv', 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Remove outer quotes from fields (but be careful not across lines)
    lines = content.split('\n')
    cleaned_lines = []
    
    for line in lines:
        if line.strip():  # Skip empty lines
            # Remove quotes around entire field contents
            cleaned_line = line.replace('","', ',').replace('",', ',').replace('",', ',')
            # Remove leading/trailing quotes
            if cleaned_line.startswith('"') and '"' in cleaned_line[1:]:
                # Find the last quote that's not the first character
                last_quote_pos = cleaned_line.rfind('"')
                if last_quote_pos > 0:
                    cleaned_line = cleaned_line[1:last_quote_pos] + ',' + cleaned_line[last_quote_pos+1:]
            
            cleaned_lines.append(cleaned_line)
        else:
            cleaned_lines.append(line)
    
    cleaned_content = '\n'.join(cleaned_lines)
    
    # Write back
    with open('screener_stocks_by_industry_with_names.csv', 'w', encoding='utf-8') as f:
        f.write(cleaned_content)
    
    print(f"✅ Manually removed quotations from CSV file")
    print(f"📊 File updated successfully")

if __name__ == "__main__":
    main()
