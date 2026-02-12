import json
import os
import pandas as pd
import numpy as np
from pipeline_utils import BASE_DIR

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
SECTOR_OUTPUT_FILE = os.path.join(BASE_DIR, "sector_analytics.json")

def calculate_rs_score(df):
    """Calculates Relative Strength Score (1-99) for each stock."""
    # Standard Momentum Weights: 40% 1Y, 20% 6M, 20% 3M, 20% 1M
    w_1y, w_6m, w_3m, w_1m = 0.4, 0.2, 0.2, 0.2
    
    for col in ['1 Year Returns(%)', '6 Month Returns(%)', '3 Month Returns(%)', '1 Month Returns(%)']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df['RS_Raw'] = (df['1 Year Returns(%)'] * w_1y) + \
                   (df['6 Month Returns(%)'] * w_6m) + \
                   (df['3 Month Returns(%)'] * w_3m) + \
                   (df['1 Month Returns(%)'] * w_1m)
    
    if not df.empty:
        # Strict rank mapping: ensure max is 99
        df['RS Rating'] = ((df['RS_Raw'].rank(pct=True, method='max') * 98) + 1).astype(int)
        df['RS Rating'] = df['RS Rating'].clip(1, 99)
    else:
        df['RS Rating'] = 0
    return df

def generate_analytics(df):
    """Generates Sector and Industry level metrics matched to UI screenshot."""
    
    def is_above(status_str, sma_key):
        if not isinstance(status_str, str): return False
        return sma_key in status_str and "Above" in status_str

    # Pre-calculate flags
    df['Above_SMA_200'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 200"))
    df['Above_SMA_50'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 50"))
    df['Above_SMA_20'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 20"))
    df['Above_EMA_21'] = df['EMA Status'].apply(lambda x: is_above(x, "EMA 20"))
    
    # helper for contribution: Performance Power = Mcap * 1yr Return
    df['Power'] = df['Market Cap(Cr.)'] * df['1 Year Returns(%)']

    # --- 1. SECTOR LEVEL (Main Bars) ---
    sector_groups = df.groupby('Sector')
    sector_list = []
    
    for sector_name, group in sector_groups:
        if sector_name == "N/A": continue
        sector_list.append({
            "Type": "Sector",
            "Name": sector_name,
            "StockCount": len(group),
            "Breadth_SMA200": round(group['Above_SMA_200'].mean() * 100, 1),
            "Breadth_SMA50": round(group['Above_SMA_50'].mean() * 100, 1),
            "Breadth_SMA20": round(group['Above_SMA_20'].mean() * 100, 1),
            "Breadth_EMA21": round(group['Above_EMA_21'].mean() * 100, 1),
            "Total_Power": group['Power'].sum()
        })
    
    sector_power_map = {item['Name']: item['Total_Power'] for item in sector_list}

    # --- 2. INDUSTRY LEVEL (Sidebar) ---
    industry_groups = df.groupby('Basic Industry')
    industry_list = []
    
    for ind_name, group in industry_groups:
        if ind_name == "N/A": continue
        parent_sector = group['Sector'].iloc[0]
        ind_power = group['Power'].sum()
        sector_total_power = sector_power_map.get(parent_sector, 1)
        
        # Contribution Calculation
        contribution = (ind_power / sector_total_power * 100) if sector_total_power != 0 else 0
        
        industry_list.append({
            "Type": "Industry",
            "Name": ind_name,
            "ParentSector": parent_sector,
            "StockCount": len(group),
            "Breadth_SMA200": round(group['Above_SMA_200'].mean() * 100, 1),
            "Breadth_SMA50": round(group['Above_SMA_50'].mean() * 100, 1),
            "Breadth_SMA20": round(group['Above_SMA_20'].mean() * 100, 1),
            "Breadth_EMA21": round(group['Above_EMA_21'].mean() * 100, 1),
            "Contribution_%": round(contribution, 1)
        })

    return {"Sectors": sector_list, "Industries": industry_list}

def main():
    if not os.path.exists(INPUT_FILE):
        print("Input file not found.")
        return

    print("Loading data for Multi-Level Analysis...")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    
    # 1. Update RS Rating in Stock JSON
    df = calculate_rs_score(df)
    
    # 2. Generate Sector/Industry Analytics
    print("Generating refined Sector & Industry analytics...")
    analytics_output = generate_analytics(df)
    
    with open(SECTOR_OUTPUT_FILE, "w") as f:
        json.dump(analytics_output, f, indent=4)

    # 3. Save Cleaned Stock JSON
    cols_to_drop = ['RS_Raw', 'Above_SMA_200', 'Above_SMA_50', 'Above_SMA_20', 'Above_EMA_21', 'Power']
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    final_stock_data = df.to_dict(orient='records')
    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_stock_data, f, indent=4)
    
    print(f"Success! Sector metrics ({len(analytics_output['Sectors'])}) and Industry metrics ({len(analytics_output['Industries'])}) saved to {SECTOR_OUTPUT_FILE}")

if __name__ == "__main__":
    main()
