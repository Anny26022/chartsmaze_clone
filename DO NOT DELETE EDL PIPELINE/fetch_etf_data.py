import sys

from pipeline_utils import SCANX_FETCH_URL, fetch_scanx_data, save_json


OUTPUT_FILE = "etf_data_response.json"
ETF_FIELDS = [
    "Isin", "OgInst", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue", "Year1RevenueGrowth",
    "NetProfitMargin", "YoYLastQtrlyProfitGrowth", "EBIDTAMargin", "Volume", "PricePerchng1year",
    "PricePerchng3year", "PricePerchng5year", "Ind_Pe", "Pb", "DivYeild", "Eps", "DaySMA50CurrentCandle",
    "DaySMA200CurrentCandle", "DayRSI14CurrentCandle", "ROCE", "MfCoCode", "Ltp", "Roe",
    "RtAwayFrom5YearHigh", "High5yr", "Sym", "PricePerchng1mon", "PricePerchng3mon", "ExpenseRatio",
    "PledgeBenefit", "Rmp"
]


def build_payload():
    # Payload for ETFs as specified by the user.
    return {
        "data": {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 1000, # Large count to get all ETFs in one go
            "fields": ETF_FIELDS,
            "params": [
                {"field": "OgInst", "op": "", "val": "ETF"},
                {"field": "Exch", "op": "", "val": "NSE"}
            ],
            "pgno": 0,
            "sorder": "desc",
            "sort": "Mcap"
        }
    }


def fetch_all_etf_data():
    print(f"Fetching ETF data from {SCANX_FETCH_URL}...")
    try:
        cleaned_data = fetch_scanx_data(build_payload())
        if not cleaned_data:
            print("Response structure might be different than expected. Check raw response.")
            return False

        save_json(OUTPUT_FILE, cleaned_data)
        print(f"Successfully fetched {len(cleaned_data)} ETFs. Saved to {OUTPUT_FILE}")
        return True
            
    except Exception as e:
        print(f"Error fetching ETF data: {e}")
        return False

if __name__ == "__main__":
    sys.exit(0 if fetch_all_etf_data() else 1)
