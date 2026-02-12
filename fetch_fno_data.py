import requests
import json
import time

def fetch_fno_flag_data():
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    
    # Payload as specified by the user with FnoFlag: 1
    # Using count: 500 to try and get all 207 in one go
    payload = {
        "data": {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 500,
            "fields": [
                "Isin", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue", "Year1RevenueGrowth", "NetProfitMargin",
                "YoYLastQtrlyProfitGrowth", "EBIDTAMargin", "volume", "PricePerchng1year", "PricePerchng3year",
                "PricePerchng5year", "Ind_Pe", "Pb", "DivYeild", "Eps", "DaySMA50CurrentCandle", "DaySMA200CurrentCandle",
                "DayRSI14CurrentCandle", "ROCE", "Ltp", "Roe", "RtAwayFrom5YearHigh", "RtAwayFrom1MonthHigh",
                "High5yr", "High3Yr", "High1Yr", "High1Wk", "Sym", "PricePerchng1mon", "PricePerchng1week",
                "PricePerchng3mon", "YearlyEarningPerShare", "OCFGrowthOnYr", "Year1CAGREPSGrowth", "NetChangeInCash",
                "FreeCashFlow", "PricePerchng2week", "DayBbUpper_Sub_BbLower", "DayATR14CurrentCandleMul_2",
                "Min5HighCurrentCandle", "Min15HighCurrentCandle", "Min5EMA50CurrentCandle", "Min15EMA50CurrentCandle",
                "Min15SMA100CurrentCandle", "Open", "BcClose", "Rmp", "PledgeBenefit"
            ],
            "params": [
                {"field": "FnoFlag", "op": "", "val": "1"},
                {"field": "OgInst", "op": "", "val": "ES"}
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

    print(f"Fetching F&O data (FnoFlag: 1) from {url}...")
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if 'data' in data and isinstance(data['data'], list):
            cleaned_stocks = data['data']
            # Save the cleaned list to a JSON file
            output_file = "fno_stocks_response.json"
            with open(output_file, "w") as f:
                json.dump(cleaned_stocks, f, indent=4)
            print(f"Successfully fetched {len(cleaned_stocks)} F&O stocks. Saved to {output_file}")
        else:
            print("Response structure might be different than expected.")
            
    except Exception as e:
        print(f"Error fetching data: {e}")

if __name__ == "__main__":
    fetch_fno_flag_data()
