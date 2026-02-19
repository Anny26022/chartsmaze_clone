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
    w_1y, w_6m, w_3m, w_1m = 0.4, 0.2, 0.2, 0.2
    
    for col in ['1 Year Returns(%)', '6 Month Returns(%)', '3 Month Returns(%)', '1 Month Returns(%)']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df['RS_Raw'] = (df['1 Year Returns(%)'] * w_1y) + \
                   (df['6 Month Returns(%)'] * w_6m) + \
                   (df['3 Month Returns(%)'] * w_3m) + \
                   (df['1 Month Returns(%)'] * w_1m)
    
    if not df.empty:
        # Professional RS Percentile: 1-99
        df['RS Rating'] = (df['RS_Raw'].rank(pct=True, method='min') * 99).apply(np.ceil).astype(int)
        df['RS Rating'] = df['RS Rating'].clip(1, 99)
    else:
        df['RS Rating'] = 0
    return df

def generate_analytics(df):
    """Generates Sector and Industry level metrics matched to UI screenshot."""
    
    def is_above(status_str, sma_key):
        if not isinstance(status_str, str): return False
        return sma_key in status_str and "Above" in status_str

    # --- 1. PRE-CALCULATE FLAGS ---
    df['Above_SMA_200'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 200"))
    df['Above_SMA_50'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 50"))
    df['Above_SMA_20'] = df['SMA Status'].apply(lambda x: is_above(x, "SMA 20"))
    
    # Near High Buckets
    df['Dist_High'] = df['% from 52W High'].apply(lambda x: abs(x) if x is not None else 999)
    
    # RS Buckets
    df['Above_RS_70'] = df['RS Rating'] >= 70
    df['Above_RS_80'] = df['RS Rating'] >= 80
    df['Above_RS_90'] = df['RS Rating'] >= 90

    # Counts for contribution
    sector_counts = df['Sector'].value_counts().to_dict()

    # --- 2. SECTOR LEVEL ---
    sector_list = []
    sector_groups = df.groupby('Sector')
    for sector_name, group in sector_groups:
        if not sector_name or sector_name == "N/A": continue
        
        sector_list.append({
            "Type": "Sector",
            "Name": sector_name,
            "StockCount": len(group),
            "Breadth_SMA200": round(group['Above_SMA_200'].mean() * 100, 1),
            "Breadth_SMA50": round(group['Above_SMA_50'].mean() * 100, 1),
            "Breadth_SMA20": round(group['Above_SMA_20'].mean() * 100, 1),
            "Breadth_RS70": round(group['Above_RS_70'].mean() * 100, 1),
            "Breadth_RS80": round(group['Above_RS_80'].mean() * 100, 1),
            "Breadth_RS90": round(group['Above_RS_90'].mean() * 100, 1),
            "NearHigh_1pc": round((group['Dist_High'] <= 1.0).mean() * 100, 1),
            "NearHigh_2pc": round((group['Dist_High'] <= 2.0).mean() * 100, 1),
            "NearHigh_5pc": round((group['Dist_High'] <= 5.0).mean() * 100, 1)
        })

    # --- 3. INDUSTRY LEVEL ---
    industry_list = []
    industry_groups = df.groupby('Basic Industry')
    for ind_name, group in industry_groups:
        if not ind_name or ind_name == "N/A": continue
        parent_sector = group['Sector'].iloc[0]
        sector_total = sector_counts.get(parent_sector, 1)
        
        # Absolute Breadth Contribution (Industry Winners / Sector Total Stocks)
        win_50 = group['Above_SMA_50'].sum()
        win_rs80 = group['Above_RS_80'].sum()
        win_nh5 = (group['Dist_High'] <= 5.0).sum()
        
        industry_list.append({
            "Type": "Industry",
            "Name": ind_name,
            "ParentSector": parent_sector,
            "StockCount": len(group),
            "Breadth_SMA50": round(group['Above_SMA_50'].mean() * 100, 1),
            "Breadth_RS80": round(group['Above_RS_80'].mean() * 100, 1),
            "Breadth_NH5": round((group['Dist_High'] <= 5.0).mean() * 100, 1),
            # Dashboard Style Contribution
            "Contribution_SMA50_%": round((win_50 / sector_total) * 100, 1),
            "Contribution_RS80_%": round((win_rs80 / sector_total) * 100, 1),
            "Contribution_NH5_%": round((win_nh5 / sector_total) * 100, 1)
        })

    return {"Sectors": sector_list, "Industries": industry_list}

def main():
    if not os.path.exists(INPUT_FILE):
        print("Input file not found.")
        return

    print("Loading data for Multi-Tab Analysis (MA, RS, NH)...")
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    
    # Global Filter (Min 300 Cr) - Disabled as per user request
    df['Market Cap(Cr.)'] = pd.to_numeric(df['Market Cap(Cr.)'], errors='coerce').fillna(0)
    # df = df[df['Market Cap(Cr.)'] >= 300].copy()
    
    # 1. Update Ranks
    df = calculate_rs_score(df)
    
    # 2. Generate Analytics
    print("Generating comprehensive analytics for all three dashboard tabs...")
    analytics_output = generate_analytics(df)
    
    with open(SECTOR_OUTPUT_FILE, "w") as f:
        json.dump(analytics_output, f, indent=4)

    # 3. Save Final Stock Master
    cols_to_drop = [
        'RS_Raw', 'Above_SMA_200', 'Above_SMA_50', 'Above_SMA_20', 
        'Above_RS_70', 'Above_RS_80', 'Above_RS_90', 'Dist_High'
    ]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(df.to_dict(orient='records'), f, indent=4)
    
    print(f"Success! Dashboard Engine complete for MA, RS, and Near-High views.")

if __name__ == "__main__":
    main()
