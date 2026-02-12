import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import io

def fetch_nse_security_list():
    # URL for the daily security list CSV
    base_url = "https://nsearchives.nseindia.com/content/equities/sec_list_{date}.csv"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "*/*"
    }

    # Start checking from today going backwards up to 7 days
    today = datetime.now()
    clean_data = []

    for i in range(8):
        check_date = today - timedelta(days=i)
        date_str = check_date.strftime("%d%m%Y")  # Format: ddmmyyyy 
        url = base_url.format(date=date_str)
        
        print(f"Checking for security list on {date_str}...")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                print(f"  Found data for {date_str}!")
                
                # Decode CSV content
                csv_content = response.content.decode('utf-8')
                
                try:
                    df = pd.read_csv(io.StringIO(csv_content))
                    
                    # Convert to list of dictionaries
                    raw_data = df.to_dict(orient='records')
                    
                    # Clean the data keys and values
                    for record in raw_data:
                        cleaned_record = {}
                        for k, v in record.items():
                            key = k.strip() if isinstance(k, str) else k
                            value = v.strip() if isinstance(v, str) else v
                            cleaned_record[key] = value
                        clean_data.append(cleaned_record)
                        
                    break  # Stop searching once we find the latest file
                    
                except Exception as parse_error:
                    print(f"  Error parsing CSV for {date_str}: {parse_error}")
                    continue

            elif response.status_code == 404:
                print(f"  No file found for {date_str} (404).")
            else:
                print(f"  Unexpected status {response.status_code} for {date_str}.")
                
        except Exception as e:
            print(f"  Error checking {date_str}: {e}")

    if clean_data:
        output_file = "complete_price_bands.json"
        with open(output_file, "w") as f:
            json.dump(clean_data, f, indent=4)
        print(f"Successfully saved {len(clean_data)} securities to {output_file}")
    else:
        print("Could not find any security list files in the last 7 days.")

if __name__ == "__main__":
    fetch_nse_security_list()
