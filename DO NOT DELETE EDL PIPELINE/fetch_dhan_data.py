import requests
import json
import os
from pipeline_utils import get_headers

def fetch_all_dhan_data():
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    
    # Paths relative to script
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(BASE_DIR, "dhan_data_response.json")
    master_map_file = os.path.join(BASE_DIR, "master_isin_map.json")

    # Payload as specified by the user
    # Trying a large count to get "all available" in one call as requested
    payload = {
        "data": {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 5000, # Large count to try and get everything
            "fields": [
                "Isin", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue", "Year1RevenueGrowth", "NetProfitMargin",
                "YoYLastQtrlyProfitGrowth", "EBIDTAMargin", "volume", "PricePerchng1year", "PricePerchng3year",
                "PricePerchng5year", "Ind_Pe", "Pb", "DivYeild", "Eps", "DaySMA50CurrentCandle", "DaySMA200CurrentCandle",
                "DayRSI14CurrentCandle", "ROCE", "Ltp", "Roe", "RtAwayFrom5YearHigh", "RtAwayFrom1MonthHigh",
                "High5yr", "High3Yr", "High1Yr", "High1Wk", "Sym", "PricePerchng1mon", "PricePerchng1week",
                "PricePerchng3mon", "YearlyEarningPerShare", "OCFGrowthOnYr", "Year1CAGREPSGrowth", "NetChangeInCash",
                "FreeCashFlow", "PricePerchng2week", "DayBbUpper_Sub_BbLower", "DayATR14CurrentCandleMul_2",
                "Min5HighCurrentCandle", "Min15HighCurrentCandle", "Min5EMA50CurrentCandle", "Min15EMA50CurrentCandle",
                "Min15SMA100CurrentCandle", "Open", "BcClose", "Rmp", "PledgeBenefit", "idxlist", "Sid"
            ],
            "params": [
                {"field": "OgInst", "op": "", "val": "ES"},
                {"field": "Exch", "op": "", "val": "NSE"}
            ],
            "pgno": 0,
            "sorder": "desc",
            "sort": "Mcap"
        }
    }

    headers = get_headers(include_origin=True)

    print(f"Fetching data from {url}...")
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if 'data' in data and isinstance(data['data'], list):
            cleaned_data = data['data']
            # Save the cleaned list to a JSON file
            with open(output_file, "w") as f:
                json.dump(cleaned_data, f, indent=4)
            print(f"Successfully fetched {len(cleaned_data)} items. Saved to {output_file}")
            
            # --- Save Master ISIN Map ---
            print("Creating Master ISIN Map...")
            master_map = []
            for item in cleaned_data:
                sym = item.get('Sym')
                isin = item.get('Isin')
                disp_sym = item.get('DispSym')
                sid = item.get('Sid')
                
                if sym and isin:
                    master_map.append({
                        "Symbol": sym,
                        "ISIN": isin,
                        "Name": disp_sym,
                        "Sid": sid
                    })
            
            # Sort for consistency
            master_map.sort(key=lambda x: x['Symbol'])
            
            with open(master_map_file, "w") as f_map:
                json.dump(master_map, f_map, indent=4)
            print(f"Successfully saved {len(master_map)} symbols (with Sid) to {master_map_file}")
        else:
            print("Response structure might be different than expected.")
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        if 'response' in locals() and response is not None:
            print(f"Response status: {response.status_code}")
            print(f"Response text: {response.text[:500]}")

if __name__ == "__main__":
    fetch_all_dhan_data()
