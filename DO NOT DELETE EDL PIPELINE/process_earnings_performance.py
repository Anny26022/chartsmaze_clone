import json
import os
import glob
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
FILINGS_DIR = "/Users/aniketmahato/Desktop/Chartsmaze/DO NOT DELETE EDL PIPELINE/company_filings"
OHLCV_DIR = "/Users/aniketmahato/Desktop/Chartsmaze/DO NOT DELETE EDL PIPELINE/ohlcv_data"
MASTER_JSON = "/Users/aniketmahato/Desktop/Chartsmaze/do not delete edl pipeline/all_stocks_fundamental_analysis.json"

def get_earnings_info(filing_path):
    """Extract latest results date and time"""
    try:
        with open(filing_path, "r") as f:
            data = json.load(f)
            filings = data.get("data", [])
            results = [f for f in filings if f.get("descriptor") == "Financial Results"]
            if not results: return None, None
            results.sort(key=lambda x: x.get("news_date", ""), reverse=True)
            return results[0].get("news_date", ""), results[0].get("descriptor")
    except Exception:
        return None, None

def calculate_earnings_metrics(csv_path, earnings_news_date):
    """Calculate returns since the earnings announcement using smart benchmarking"""
    if not earnings_news_date:
        return 0.0, 0.0
    
    try:
        # news_date format: "2026-01-27 20:17:25"
        date_part = earnings_news_date.split(" ")[0]
        time_part = earnings_news_date.split(" ")[1] if " " in earnings_news_date else "00:00:00"
        
        target_date = pd.to_datetime(date_part)
        hour = int(time_part.split(":")[0])
        minute = int(time_part.split(":")[1])
        
        df = pd.read_csv(csv_path)
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Latest trading session
        latest_price = df.iloc[-1]['Close']

        # Determine if news hit after hours (Post-Market)
        # In India, market closes at 15:30.
        is_after_hours = (hour > 15) or (hour == 15 and minute >= 30)
        
        if is_after_hours:
            # Benchmark is the close of the announcement day itself
            pre_news_df = df[df['Date'] <= target_date]
            post_news_df = df[df['Date'] > target_date]
        else:
            # Benchmark is the close of the day STRICTLY BEFORE
            pre_news_df = df[df['Date'] < target_date]
            post_news_df = df[df['Date'] >= target_date]
            
        if pre_news_df.empty or post_news_df.empty:
            # Fallback
            if post_news_df.empty: return 0.0, 0.0
            base_price = post_news_df.iloc[0]['Close']
        else:
            base_price = pre_news_df.iloc[-1]['Close']
            
        # 1. Returns since Earnings (%)
        returns_since = ((latest_price - base_price) / base_price) * 100
        
        # 2. Max Returns since Earnings (%)
        max_high = post_news_df['High'].max()
        max_returns = ((max_high - base_price) / base_price) * 100
        
        return round(returns_since, 2), round(max_returns, 2)
    except Exception:
        return 0.0, 0.0

def main():
    print("Loading master analysis data...")
    try:
        with open(MASTER_JSON, "r") as f:
            analysis_data = json.load(f)
    except Exception as e:
        print(f"Error loading {MASTER_JSON}: {e}")
        return

    print("Analyzing filings and calculating earnings metrics...")
    
    for stock in analysis_data:
        symbol = stock.get("Symbol")
        filing_file = os.path.join(FILINGS_DIR, f"{symbol}_filings.json")
        ohlcv_file = os.path.join(OHLCV_DIR, f"{symbol}.csv")
        
        # 1. Get Earnings Info
        earnings_news_date, _ = get_earnings_info(filing_file)
        stock["Quarterly Results Date"] = earnings_news_date.split(" ")[0] if earnings_news_date else "N/A"
        
        # 2. Calculate Metrics
        ret, max_ret = calculate_earnings_metrics(ohlcv_file, earnings_news_date)
        stock["Returns since Earnings(%)"] = ret
        stock["Max Returns since Earnings(%)"] = max_ret

    # Save update
    with open(MASTER_JSON, "w") as f:
        json.dump(analysis_data, f, indent=4)
        
    print(f"Successfully updated earnings metrics for {len(analysis_data)} stocks.")

if __name__ == "__main__":
    main()
