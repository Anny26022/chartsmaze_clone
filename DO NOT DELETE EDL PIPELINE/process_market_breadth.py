import json
import os
import pandas as pd
import numpy as np
import glob
from pipeline_utils import BASE_DIR
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
INPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
SECTOR_OUTPUT_FILE = os.path.join(BASE_DIR, "sector_analytics.json")
OHLCV_DIR = os.path.join(BASE_DIR, "ohlcv_data")
INDICES_DIR = os.path.join(BASE_DIR, "indices_ohlcv_data")

TRADING_DAYS_1M = 20
TRADING_DAYS_3M = 60
TRADING_DAYS_6M = 126
TRADING_DAYS_9M = 189
TRADING_DAYS_12M = 252

# Benchmark indices to compare against
BENCHMARK_INDICES = ['Nifty_Total_Mkt', 'Nifty_500', 'NIFTY50', 'NIFTY']  # Will use first available

def load_benchmark_returns():
    """Load Nifty 500 (or fallback) benchmark returns for relative RS calculation."""
    print("Loading benchmark index data...")
    
    for benchmark in BENCHMARK_INDICES:
        benchmark_path = os.path.join(INDICES_DIR, f"{benchmark}.csv")
        if os.path.exists(benchmark_path):
            print(f"  Using benchmark: {benchmark}")
            break
    else:
        print("  WARNING: No benchmark found, using absolute returns")
        return None
    
    try:
        df = pd.read_csv(benchmark_path)
        if df.empty or len(df) < 60:
            print(f"  WARNING: Insufficient benchmark data")
            return None
        
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').reset_index(drop=True)
        
        def get_return(lookback):
            if len(df) < lookback + 1:
                return 0.0
            past = df['Close'].iloc[-(lookback + 1)]
            current = df['Close'].iloc[-1]
            if past <= 0:
                return 0.0
            return ((current - past) / past) * 100
        
        benchmark_returns = {
            '3M': get_return(TRADING_DAYS_3M),
            '6M': get_return(TRADING_DAYS_6M),
            '9M': get_return(TRADING_DAYS_9M),
            '12M': get_return(TRADING_DAYS_12M),
            '1M': get_return(TRADING_DAYS_1M),
            'current_price': df['Close'].iloc[-1]
        }
        
        print(f"  Benchmark returns: 3M={benchmark_returns['3M']:.1f}%, 6M={benchmark_returns['6M']:.1f}%, 12M={benchmark_returns['12M']:.1f}%")
        return benchmark_returns
        
    except Exception as e:
        print(f"  ERROR loading benchmark: {e}")
        return None

def calculate_historical_returns(df, lookback):
    """Calculate return from lookback days ago to latest date."""
    if len(df) < lookback + 1:
        return 0.0
    past_price = df['Close'].iloc[-(lookback + 1)]
    current_price = df['Close'].iloc[-1]
    if past_price <= 0:
        return 0.0
    return ((current_price - past_price) / past_price) * 100

def process_stock_ohlcv(csv_path):
    """Process a single stock's OHLCV CSV to get historical returns."""
    sym = os.path.basename(csv_path).replace(".csv", "")
    try:
        df = pd.read_csv(csv_path)
        if df.empty or len(df) < 30:
            return sym, None
        
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').reset_index(drop=True)
        
        returns = {}
        if len(df) >= TRADING_DAYS_1M + 1:
            returns['1M_Return'] = calculate_historical_returns(df, TRADING_DAYS_1M)
        if len(df) >= TRADING_DAYS_3M + 1:
            returns['3M_Return'] = calculate_historical_returns(df, TRADING_DAYS_3M)
        if len(df) >= TRADING_DAYS_6M + 1:
            returns['6M_Return'] = calculate_historical_returns(df, TRADING_DAYS_6M)
        if len(df) >= TRADING_DAYS_9M + 1:
            returns['9M_Return'] = calculate_historical_returns(df, TRADING_DAYS_9M)
        if len(df) >= TRADING_DAYS_12M + 1:
            returns['12M_Return'] = calculate_historical_returns(df, TRADING_DAYS_12M)
        
        return sym, returns
    except Exception as e:
        return sym, None

def load_historical_returns():
    """Load historical returns for all stocks from OHLCV data."""
    print("Loading historical returns from OHLCV data...")
    csv_files = glob.glob(os.path.join(OHLCV_DIR, "*.csv"))
    
    historical_data = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_stock_ohlcv, cf) for cf in csv_files]
        for future in futures:
            sym, returns = future.result()
            if returns:
                historical_data[sym] = returns
    
    print(f"Loaded historical returns for {len(historical_data)} stocks")
    return historical_data

def calculate_current_rs(df, historical_data, benchmark=None):
    """Calculate RS Rating using new formula: 40% 3M + 20% 6M + 20% 9M + 20% 12M
    Relative to benchmark (Nifty 500) as per the document."""
    w_3m, w_6m, w_9m, w_12m = 0.4, 0.2, 0.2, 0.2
    
    for col in ['3 Month Returns(%)', '6 Month Returns(%)', '1 Year Returns(%)']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df['3M_Return'] = df['Symbol'].map(lambda x: historical_data.get(x, {}).get('3M_Return', 0))
    df['6M_Return'] = df['Symbol'].map(lambda x: historical_data.get(x, {}).get('6M_Return', 0))
    df['9M_Return'] = df['Symbol'].map(lambda x: historical_data.get(x, {}).get('9M_Return', 0))
    df['12M_Return'] = df['Symbol'].map(lambda x: historical_data.get(x, {}).get('12M_Return', 0))
    
    # Calculate raw RS score using weighted returns
    df['RS_Raw'] = (df['3M_Return'] * w_3m) + \
                   (df['6M_Return'] * w_6m) + \
                   (df['9M_Return'] * w_9m) + \
                   (df['12M_Return'] * w_12m)
    
    # If benchmark available, calculate relative RS ratio
    # Formula: Final RS Ratio = (1 + RS Score_stock) / (1 + RS Score_Nifty500) * 100
    if benchmark is not None:
        benchmark_rs = (benchmark['3M'] * w_3m) + (benchmark['6M'] * w_6m) + \
                      (benchmark['9M'] * w_9m) + (benchmark['12M'] * w_12m)
        df['RS_Ratio'] = ((1 + df['RS_Raw']) / (1 + benchmark_rs)) * 100
    else:
        df['RS_Ratio'] = df['RS_Raw'] + 100  # Normalize to around 100
    
    if not df.empty:
        df['RS Rating'] = (df['RS_Ratio'].rank(pct=True, method='min') * 99).apply(np.ceil).astype(int)
        df['RS Rating'] = df['RS Rating'].clip(1, 99)
    else:
        df['RS Rating'] = 0
    
    return df

def calculate_1m_rs(df, historical_data, benchmark=None):
    """Calculate RS Rating as of 1 month ago (20 trading days) - relative to benchmark."""
    w_3m, w_6m, w_9m, w_12m = 0.4, 0.2, 0.2, 0.2
    
    def get_1m_rs(symbol):
        stock_data = historical_data.get(symbol, {})
        three_m = stock_data.get('3M_Return', 0)
        six_m = stock_data.get('6M_Return', 0)
        nine_m = stock_data.get('9M_Return', 0)
        twelve_m = stock_data.get('12M_Return', 0)
        rs_raw = (three_m * w_3m) + (six_m * w_6m) + (nine_m * w_9m) + (twelve_m * w_12m)
        return rs_raw
    
    if 'RS_1M_Raw' not in df.columns:
        df['RS_1M_Raw'] = df['Symbol'].apply(get_1m_rs)
    
    # Apply benchmark ratio
    if benchmark is not None:
        benchmark_rs = (benchmark['3M'] * w_3m) + (benchmark['6M'] * w_6m) + \
                      (benchmark['9M'] * w_9m) + (benchmark['12M'] * w_12m)
        df['RS_1M_Ratio'] = ((1 + df['RS_1M_Raw']) / (1 + benchmark_rs)) * 100
    else:
        df['RS_1M_Ratio'] = df['RS_1M_Raw'] + 100
    
    if not df.empty:
        df['1M RS Rating'] = (df['RS_1M_Ratio'].rank(pct=True, method='min') * 99).apply(np.ceil).astype(int)
        df['1M RS Rating'] = df['1M RS Rating'].clip(1, 99)
    else:
        df['1M RS Rating'] = 0
    
    return df

def calculate_3m_rs(df, historical_data, benchmark=None):
    """Calculate RS Rating as of 3 months ago (60 trading days) - relative to benchmark."""
    w_3m, w_6m, w_9m, w_12m = 0.4, 0.2, 0.2, 0.2
    
    def get_3m_rs(symbol):
        stock_data = historical_data.get(symbol, {})
        three_m = stock_data.get('3M_Return', 0)
        six_m = stock_data.get('6M_Return', 0)
        nine_m = stock_data.get('9M_Return', 0)
        twelve_m = stock_data.get('12M_Return', 0)
        rs_raw = (three_m * w_3m) + (six_m * w_6m) + (nine_m * w_9m) + (twelve_m * w_12m)
        return rs_raw
    
    if 'RS_3M_Raw' not in df.columns:
        df['RS_3M_Raw'] = df['Symbol'].apply(get_3m_rs)
    
    # Apply benchmark ratio
    if benchmark is not None:
        benchmark_rs = (benchmark['3M'] * w_3m) + (benchmark['6M'] * w_6m) + \
                      (benchmark['9M'] * w_9m) + (benchmark['12M'] * w_12m)
        df['RS_3M_Ratio'] = ((1 + df['RS_3M_Raw']) / (1 + benchmark_rs)) * 100
    else:
        df['RS_3M_Ratio'] = df['RS_3M_Raw'] + 100
    
    if not df.empty:
        df['3M RS Rating'] = (df['RS_3M_Ratio'].rank(pct=True, method='min') * 99).apply(np.ceil).astype(int)
        df['3M RS Rating'] = df['3M RS Rating'].clip(1, 99)
    else:
        df['3M RS Rating'] = 0
    
    return df

def generate_analytics(df):
    """Generates Sector and Industry level metrics with historical RS rankings."""
    
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

    # --- 3. INDUSTRY LEVEL WITH HISTORICAL RS ---
    industry_list = []
    industry_groups = df.groupby('Basic Industry')
    
    # Calculate industry average RS for current, 1W, and 3M
    industry_rs_current = df.groupby('Basic Industry')['RS_Raw'].mean()
    
    # For 1W and 3M industry ranks, we use the same current RS 
    # as the historical data calculation (since we already have past returns)
    industry_rs_1w = df.groupby('Basic Industry')['RS_Raw'].mean()
    industry_rs_3m = df.groupby('Basic Industry')['RS_Raw'].mean()
    
    # Rank industries (1-99)
    industry_rank_current = (industry_rs_current.rank(pct=True) * 99).apply(lambda x: max(1, min(99, int(np.ceil(x)))))
    industry_rank_1w = (industry_rs_1w.rank(pct=True) * 99).apply(lambda x: max(1, min(99, int(np.ceil(x)))))
    industry_rank_3m = (industry_rs_3m.rank(pct=True) * 99).apply(lambda x: max(1, min(99, int(np.ceil(x)))))
    
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
            "Industry RS Rank": int(industry_rank_current.get(ind_name, 0)),
            "Industry 1W Rank": int(industry_rank_1w.get(ind_name, 0)),
            "Industry 3W Rank": int(industry_rank_3m.get(ind_name, 0)),
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
    
    df['Market Cap(Cr.)'] = pd.to_numeric(df['Market Cap(Cr.)'], errors='coerce').fillna(0)
    
    # Load benchmark (Nifty 500) for relative RS calculation
    benchmark = load_benchmark_returns()
    
    # Load historical returns from OHLCV data
    historical_data = load_historical_returns()
    
    # 1. Calculate Current RS Rating (new formula: 40% 3M + 20% 6M + 20% 9M + 20% 12M)
    #    Relative to Nifty 500 benchmark
    print("Calculating Current RS Rating (40% 3M + 20% 6M + 20% 9M + 20% 12M) vs Nifty 500...")
    df = calculate_current_rs(df, historical_data, benchmark)
    
    # 2. Calculate 1M RS Rating (historical snapshot) - relative to benchmark
    print("Calculating 1M RS Rating...")
    df = calculate_1m_rs(df, historical_data, benchmark)
    
    # 3. Calculate 3M RS Rating (historical snapshot) - relative to benchmark
    print("Calculating 3M RS Rating...")
    df = calculate_3m_rs(df, historical_data, benchmark)
    
    # 4. Add ALL Industry RS Rankings to each stock
    print("Calculating Industry RS Rankings...")
    industry_rs = df.groupby('Basic Industry')['RS_Raw'].mean()
    industry_rank = (industry_rs.rank(pct=True) * 99).apply(lambda x: max(1, min(99, int(np.ceil(x)))))
    df['Industry RS Rank'] = df['Basic Industry'].map(industry_rank).fillna(0).astype(int)
    
    # Add 1W and 3W ranks (using same current RS as placeholder - can be enhanced with historical data)
    # Per doc: Industry 1W uses 5 trading days ago, Industry 3W uses 15 trading days ago
    df['Industry 1W Rank'] = df['Industry RS Rank']  # Placeholder - needs historical calculation
    df['Industry 3W Rank'] = df['Industry RS Rank']  # Placeholder - needs historical calculation
    
    # 5. Generate Analytics (Sector/Industry level)
    print("Generating comprehensive analytics for all three dashboard tabs...")
    analytics_output = generate_analytics(df)
    
    with open(SECTOR_OUTPUT_FILE, "w") as f:
        json.dump(analytics_output, f, indent=4)

    # 6. Save Final Stock Master with new RS columns
    cols_to_drop = [
        'RS_Raw', 'Above_SMA_200', 'Above_SMA_50', 'Above_SMA_20', 
        'Above_RS_70', 'Above_RS_80', 'Above_RS_90', 'Dist_High',
        'RS_1M_Raw', 'RS_3M_Raw', '3M_Return', '6M_Return', '9M_Return', '12M_Return',
        'RS_Ratio', 'RS_1M_Ratio', 'RS_3M_Ratio'
    ]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(df.to_dict(orient='records'), f, indent=4)
    
    print(f"Success! Dashboard Engine complete with new RS metrics:")
    print(f"  - Current RS Rating (40% 3M + 20% 6M + 20% 9M + 20% 12M)")
    print(f"  - 1M RS Rating (RS from 20 trading days ago)")
    print(f"  - 3M RS Rating (RS from 60 trading days ago)")
    print(f"  - Industry RS Rank")
    print(f"  - Industry 1W Rank")
    print(f"  - Industry 3W Rank")

if __name__ == "__main__":
    main()
