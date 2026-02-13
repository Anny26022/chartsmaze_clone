import os
import json
import pandas as pd
import glob
import numpy as np
import gzip
from datetime import datetime
from pipeline_utils import BASE_DIR

# --- Configuration ---
SYMBOL_OHLCV_DIR = os.path.join(BASE_DIR, "ohlcv_data")
INDEX_OHLCV_DIR = os.path.join(BASE_DIR, "indices_ohlcv_data")
MASTER_STOCKS_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
OUTPUT_CSV = os.path.join(BASE_DIR, "market_breadth.csv")
OUTPUT_GZ = os.path.join(BASE_DIR, "market_breadth.json.gz") # Matching user requested file naming convention

# Max days to look back (250 trading days ~ 1 year)
LOOKBACK_DAYS = 250

def calculate_historical_breadth():
    print("⏳ Loading master stock list (300Cr filter)...")
    if not os.path.exists(MASTER_STOCKS_FILE):
        print("Error: Run bulk_market_analyzer first.")
        return

    with open(MASTER_STOCKS_FILE, "r") as f:
        stocks_data = json.load(f)
    
    # Only use stocks we are already tracking (MCAP >= 300)
    valid_symbols = {s['Symbol'] for s in stocks_data}
    print(f"Targeting {len(valid_symbols)} quality stocks for historical breadth.")

    # 1. Determine Master Timeline (Last 250 days of Nifty 50)
    nifty_path = os.path.join(INDEX_OHLCV_DIR, "NIFTY.csv")
    if not os.path.exists(nifty_path):
        print("Error: NIFTY index data not found.")
        return
    
    nifty_df = pd.read_csv(nifty_path)
    timeline = nifty_df['Date'].tail(LOOKBACK_DAYS).tolist()
    date_to_idx = {date: i for i, date in enumerate(timeline)}
    num_days = len(timeline)

    # 2. Matrices to store daily flags (Rows=Days, Cols=Stocks)
    # Using arrays for memory efficiency
    advances = np.zeros(num_days)
    declines = np.zeros(num_days)
    above_200ma = np.zeros(num_days)
    above_50ma = np.zeros(num_days)
    above_20ma = np.zeros(num_days)
    above_10ma = np.zeros(num_days)
    up_4pc = np.zeros(num_days)
    down_4pc = np.zeros(num_days)
    high_52w = np.zeros(num_days)
    low_52w = np.zeros(num_days)
    vol_plus = np.zeros(num_days)
    vol_minus = np.zeros(num_days)

    print("🧬 Processing stock-level history...")
    csv_files = glob.glob(os.path.join(SYMBOL_OHLCV_DIR, "*.csv"))
    
    processed_count = 0
    for csv_path in csv_files:
        symbol = os.path.basename(csv_path).replace(".csv", "")
        if symbol not in valid_symbols: continue
        
        try:
            df = pd.read_csv(csv_path)
            if df.empty or len(df) < 5: continue
            
            # Align with timeline
            df = df[df['Date'].isin(timeline)].copy()
            if df.empty: continue
            
            # Re-read full history for technicals to avoid edge effects
            full_df = pd.read_csv(csv_path)
            full_df['SMA_10'] = full_df['Close'].rolling(10).mean()
            full_df['SMA_20'] = full_df['Close'].rolling(20).mean()
            full_df['SMA_50'] = full_df['Close'].rolling(50).mean()
            full_df['SMA_200'] = full_df['Close'].rolling(200).mean()
            full_df['Vol_SMA_20'] = full_df['Volume'].rolling(20).mean()
            full_df['H_52W'] = full_df['High'].rolling(252).max()
            full_df['L_52W'] = full_df['Low'].rolling(252).min()
            full_df['Prev_Close'] = full_df['Close'].shift(1)
            full_df['Daily_Ret'] = ((full_df['Close'] - full_df['Prev_Close']) / full_df['Prev_Close']) * 100

            # Filter back to timeline
            analysis_df = full_df[full_df['Date'].isin(timeline)]
            
            for _, row in analysis_df.iterrows():
                idx = date_to_idx.get(row['Date'])
                if idx is None: continue
                
                # Metrics Calculation
                if row['Close'] > row['Prev_Close']: advances[idx] += 1
                if row['Close'] < row['Prev_Close']: declines[idx] += 1
                
                if row['Close'] > row['SMA_200']: above_200ma[idx] += 1
                if row['Close'] > row['SMA_50']: above_50ma[idx] += 1
                if row['Close'] > row['SMA_20']: above_20ma[idx] += 1
                if row['Close'] > row['SMA_10']: above_10ma[idx] += 1
                
                if row['Daily_Ret'] >= 4: up_4pc[idx] += 1
                if row['Daily_Ret'] <= -4: down_4pc[idx] += 1
                
                if row['High'] >= row['H_52W']: high_52w[idx] += 1
                if row['Low'] <= row['L_52W']: low_52w[idx] += 1
                
                if row['Volume'] > row['Vol_SMA_20']: vol_plus[idx] += 1
                else: vol_minus[idx] += 1
            
            processed_count += 1
        except:
            continue

    print(f"✅ Analyzed {processed_count} stocks. Merging with Index data...")

    # 3. Handle Indices
    index_data = {}
    for label, filename in {
        "Nifty Midcap 150": "NIFTY_MIDCAP_150.csv",
        "Nifty Smallcap 250": "NIFTY_SMALLCAP_250.csv",
        "Nifty Midsmallcap 400": "NIFTY_MIDSMALLCAP_400.csv",
        "Nifty 50": "NIFTY.csv",
        "Nifty 500": "NIFTY_500.csv"
    }.items():
        path = os.path.join(INDEX_OHLCV_DIR, filename)
        if os.path.exists(path):
            idf = pd.read_csv(path)
            idf = idf[idf['Date'].isin(timeline)]
            price_map = idf.set_index('Date')['Close'].to_dict()
            index_data[label] = [round(price_map.get(d, 0), 2) for d in timeline]
        else:
            index_data[label] = [0] * num_days

    # 4. Assemble Final CSV Rows
    # Labels must match the dashboard sample EXACTLY
    def to_csv_row(label, values):
        return f"{label}," + ",".join(map(str, values))

    rows = []
    rows.append("Type of Info," + ",".join(timeline))
    
    # Values as integers where appropriate, others as per sample
    rows.append(to_csv_row("Up by 4% Today", up_4pc.astype(int)))
    rows.append(to_csv_row("Down by 4% Today", down_4pc.astype(int)))
    
    # Ratios (A/D 5D and 10D - using simple moving average of daily A/D counts)
    def calc_ratio(adv, dec, window):
        r = []
        for i in range(len(adv)):
            start = max(0, i - window + 1)
            sum_adv = sum(adv[start:i+1])
            sum_dec = sum(dec[start:i+1])
            ratio = round(sum_adv / sum_dec, 2) if sum_dec > 0 else 1.0
            r.append(ratio)
        return r

    rows.append(to_csv_row("5 Day Ratio", calc_ratio(advances, declines, 5)))
    rows.append(to_csv_row("10 Day Ratio", calc_ratio(advances, declines, 10)))
    
    # Placeholders for monthly/quarterly (matching sample rows we saw in 'cut')
    rows.append(to_csv_row("Up by 25% in Month", [0] * num_days))
    rows.append(to_csv_row("Down by 25% in Month", [0] * num_days))
    rows.append(to_csv_row("Up by 50% in Month", [0] * num_days))
    rows.append(to_csv_row("Down by 50% in Month", [0] * num_days))
    rows.append(to_csv_row("Up by 13% in 34 Days", [0] * num_days))
    rows.append(to_csv_row("Down by 13% in 34 Days", [0] * num_days))
    rows.append(to_csv_row("Up by 25% in Quarter", [0] * num_days))
    rows.append(to_csv_row("Down by 25% in Quarter", [0] * num_days))

    # Breadth %
    total_tracked = max(processed_count, 1)
    rows.append(to_csv_row("Above 200MA %", np.round(above_200ma / total_tracked * 100, 1)))
    rows.append(to_csv_row("Above 50MA %", np.round(above_50ma / total_tracked * 100, 1)))
    rows.append(to_csv_row("Above 20MA %", np.round(above_20ma / total_tracked * 100, 1)))
    rows.append(to_csv_row("Above 10MA %", np.round(above_10ma / total_tracked * 100, 1)))
    
    rows.append(to_csv_row("Reached 52w High", high_52w.astype(int)))
    rows.append(to_csv_row("Reached 52w Low", low_52w.astype(int)))
    
    rows.append(to_csv_row("Volume greater than 20Day Average", vol_plus.astype(int)))
    rows.append(to_csv_row("Volume less than 20Day Average", vol_minus.astype(int)))
    
    # Nifty 500 RSI logic (Placeholder)
    rows.append(to_csv_row("Nifty 500 % of W&M RSI > 60", [0] * num_days))
    
    rows.append(to_csv_row("Advances", advances.astype(int)))
    rows.append(to_csv_row("Declines", declines.astype(int)))

    # Price Rows
    for label, prices in index_data.items():
        rows.append(to_csv_row(label, prices))

    # 5. Save Output
    final_output = "\n".join(rows)
    with open(OUTPUT_CSV, "w") as f:
        f.write(final_output)

    print(f"🚀 Market Breadth Historical Data generated: {OUTPUT_CSV}")

if __name__ == "__main__":
    calculate_historical_breadth()
