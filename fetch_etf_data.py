import requests
import json
import os

def fetch_all_etf_data():
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    
    # Payload for ETFs as specified by the user
    payload = {
        "data": {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 1000, # Large count to get all ETFs in one go
            "fields": [
                "Isin", "OgInst", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue", "Year1RevenueGrowth",
                "NetProfitMargin", "YoYLastQtrlyProfitGrowth", "EBIDTAMargin", "Volume", "PricePerchng1year",
                "PricePerchng3year", "PricePerchng5year", "Ind_Pe", "Pb", "DivYeild", "Eps", "DaySMA50CurrentCandle",
                "DaySMA200CurrentCandle", "DayRSI14CurrentCandle", "ROCE", "MfCoCode", "Ltp", "Roe",
                "RtAwayFrom5YearHigh", "High5yr", "Sym", "PricePerchng1mon", "PricePerchng3mon", "ExpenseRatio",
                "PledgeBenefit", "Rmp"
            ],
            "params": [
                {"field": "OgInst", "op": "", "val": "ETF"},
                {"field": "Exch", "op": "", "val": "NSE"}
            ],
            "pgno": 0,
            "sorder": "desc",
            "sort": "Mcap"
        }
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://scanx.dhan.co",
        "Referer": "https://scanx.dhan.co/"
    }

    print(f"Fetching ETF data from {url}...")
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if 'data' in data and isinstance(data['data'], list):
            cleaned_data = data['data']
            # Save the cleaned list to a JSON file
            output_file = "etf_data_response.json"
            with open(output_file, "w") as f:
                json.dump(cleaned_data, f, indent=4)
            print(f"Successfully fetched {len(cleaned_data)} ETFs. Saved to {output_file}")
        else:
            print("Response structure might be different than expected. Check raw response.")
            
    except Exception as e:
        print(f"Error fetching ETF data: {e}")

if __name__ == "__main__":
    fetch_all_etf_data()
