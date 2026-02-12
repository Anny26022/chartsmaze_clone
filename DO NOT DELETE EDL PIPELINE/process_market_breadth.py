import json
import os
import pandas as pd
import numpy as np
from pipeline_utils import BASE_DIR

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")

def calculate_rs_score(df):
    """Calculates Relative Strength Score (1-99) based on weighted performance."""
    # Weights for performance (Standard semi-log weighted returns pattern)
    # We use the returns already available in our master JSON
    w_1y = 0.4
    w_6m = 0.3
    w_3m = 0.2
    w_1m = 0.1
    
    # Clean data: Replace NaNs with 0
    df['1 Year Returns(%)'] = pd.to_numeric(df['1 Year Returns(%)'], errors='coerce').fillna(0)
    df['6 Month Returns(%)'] = pd.to_numeric(df['6 Month Returns(%)'], errors='coerce').fillna(0)
    df['3 Month Returns(%)'] = pd.to_numeric(df['3 Month Returns(%)'], errors='coerce').fillna(0)
    df['1 Month Returns(%)'] = pd.to_numeric(df['1 Month Returns(%)'], errors='coerce').fillna(0)
    
    # Calculate Weighted RS Score
    df['RS_Raw'] = (df['1 Year Returns(%)'] * w_1y) + \
                   (df['6 Month Returns(%)'] * w_6m) + \
                   (df['3 Month Returns(%)'] * w_3m) + \
                   (df['1 Month Returns(%)'] * w_1m)
    
    # Rank them from 1 to 99 (Percentile)
    # 99 means top 1% of the market
    if not df.empty:
        df['RS Rating'] = (df['RS_Raw'].rank(pct=True) * 98 + 1).astype(int)
    else:
        df['RS Rating'] = 0
        
    return df

def calculate_industry_metrics(df):
    """Calculates Industry Breadth and Sector Contribution %."""
    
    # Add helper columns for Moving Average status if missing
    # In our JSON, SMA Status is a string: "SMA 20: Above (5.6%) | ..."
    # We'll extract the simple Above/Below logic
    def is_above(status_str, sma_key):
        if not isinstance(status_str, str): return False
        parts = status_str.split(" | ")
        for p in parts:
            if sma_key in p and "Above" in p:
                return True
        return False

    df['Above_SMA_200'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 200"))
    df['Above_SMA_50'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 50"))
    df['Above_SMA_20'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 20"))
    df['Above_EMA_21'] = df['EMA Status'].apply(lambda x: is_above(x, "EMA 20")) # EMA 20/21 is often interchangeable in signals
    
    # 1. INDUSTRY BREADTH (% of stocks in industry above specific averages)
    industry_groups = df.groupby('Basic Industry')
    
    breadth_200 = (industry_groups['Above_SMA_200'].mean() * 100).to_dict()
    breadth_50 = (industry_groups['Above_SMA_50'].mean() * 100).to_dict()
    breadth_20 = (industry_groups['Above_SMA_20'].mean() * 100).to_dict()
    breadth_21_ema = (industry_groups['Above_EMA_21'].mean() * 100).to_dict()

    # 2. SECTOR CONTRIBUTION
    # Power = Market Cap * 1yr Return (Simple performance contribution proxy)
    df['Mcap_Weighted_Power'] = df['Market Cap(Cr.)'] * df['1 Year Returns(%)']
    
    industry_power = industry_groups['Mcap_Weighted_Power'].sum()
    sector_mapping = df.set_index('Basic Industry')['Sector'].to_dict()
    
    # Map industries to sectors to find total sector power
    ind_to_sector_df = pd.DataFrame({
        'Industry': industry_power.index,
        'Industry_Power': industry_power.values,
        'Sector': [sector_mapping.get(ind, "N/A") for ind in industry_power.index]
    })
    
    sector_total_power = ind_to_sector_df.groupby('Sector')['Industry_Power'].sum().to_dict()
    
    # Calculate % contribution of industry to its sector
    def get_contribution(row):
        total_s_power = sector_total_power.get(row['Sector'], 0)
        if total_s_power == 0: return 0.0
        return (row['Industry_Power'] / total_s_power) * 100

    ind_to_sector_df['Contribution_%'] = ind_to_sector_df.apply(get_contribution, axis=1)
    contribution_map = ind_to_sector_df.set_index('Industry')['Contribution_%'].to_dict()

    # Apply back to dataframe
    df['Industry Breadth(SMA 200)'] = df['Basic Industry'].map(breadth_200).round(1)
    df['Industry Breadth(SMA 50)'] = df['Basic Industry'].map(breadth_50).round(1)
    df['Industry Breadth(SMA 20)'] = df['Basic Industry'].map(breadth_20).round(1)
    df['Industry Breadth(EMA 21)'] = df['Basic Industry'].map(breadth_21_ema).round(1)
    df['Industry Contribution to Sector(%)'] = df['Basic Industry'].map(contribution_map).round(1)

    return df

def main():
    if not os.path.exists(INPUT_FILE):
        print("Input file not found.")
        return

    print("Loading data for Breadth and RS analysis...")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    # Perform Calculations
    print("Calculating RS Ratings (1-99)...")
    df = calculate_rs_score(df)
    
    print("Calculating Industry Breadth and Sector Contribution...")
    df = calculate_industry_metrics(df)

    # Clean up helper columns
    cols_to_drop = ['RS_Raw', 'Above_SMA_200', 'Above_SMA_50', 'Above_SMA_20', 'Above_EMA_21', 'Mcap_Weighted_Power']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

    # Convert back to JSON
    result = df.to_dict(orient='records')
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=4)
    
    print(f"Successfully updated master JSON with RS Ratings and Industry Breadth.")

if __name__ == "__main__":
    main()
