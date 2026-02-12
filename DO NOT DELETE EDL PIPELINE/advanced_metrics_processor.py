import pandas as pd
import json
import os
import glob
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_INPUT = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
PRICE_BANDS_FILE = os.path.join(BASE_DIR, "complete_price_bands.json")
OHLCV_DIR = os.path.join(BASE_DIR, "ohlcv_data")
JSON_OUTPUT = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")

def calculate_ema(series, periods):
    return series.ewm(span=periods, adjust=False).mean()

def process_symbol_csv(csv_path):
    sym = os.path.basename(csv_path).replace(".csv", "")
    try:
        df = pd.read_csv(csv_path)
        if df.empty or len(df) < 5:
            return sym, None

        # Ensure numeric
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.dropna()
        if df.empty: return sym, None

        # Latest row
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        # --- Calculations ---
        
        # 1. ATH
        ath = df['High'].max()
        pct_from_ath = ((ath - latest['Close']) / ath) * 100 if ath > 0 else 0
        
        # 2. Gap Up % and Day Range %
        gap_up_pct = ((latest['Open'] - prev['Close']) / prev['Close']) * 100 if prev['Close'] > 0 else 0
        day_range_pct = ((latest['High'] - latest['Low']) / latest['Low']) * 100 if latest['Low'] > 0 else 0
        
        # 3. ADR (Average Daily Range)
        df['Daily_Range_Pct'] = ((df['High'] - df['Low']) / df['Low']) * 100
        adr_5 = df['Daily_Range_Pct'].tail(5).mean()
        adr_14 = df['Daily_Range_Pct'].tail(14).mean()
        adr_20 = df['Daily_Range_Pct'].tail(20).mean()
        adr_30 = df['Daily_Range_Pct'].tail(30).mean()

        # 4. Returns & Low Benchmarks
        # 6 Month Return (~126 trading days)
        price_6m_ago = df['Close'].iloc[-126] if len(df) >= 126 else df['Close'].iloc[0]
        returns_6m = ((latest['Close'] - price_6m_ago) / price_6m_ago) * 100
        
        # 52W Low (~252 trading days)
        low_52w = df['Low'].tail(252).min()
        pct_from_52w_low = ((latest['Close'] - low_52w) / low_52w) * 100 if low_52w > 0 else 0

        # 5. Volume Metrics
        df['Turnover_Cr'] = (df['Close'] * df['Volume']) / 10000000 
        avg_rupee_vol_30 = df['Turnover_Cr'].tail(30).mean()
        
        avg_vol_20 = df['Volume'].tail(21).iloc[:-1].mean()
        rvol = latest['Volume'] / avg_vol_20 if avg_vol_20 > 0 else 0
        
        df['EMA_Vol_200'] = calculate_ema(df['Volume'], 200)
        ema_vol_200_latest = df['EMA_Vol_200'].iloc[-1]
        
        # % from 52W High of 200D EMA Volume
        ema_vol_200_52w_high = df['EMA_Vol_200'].tail(252).max()
        pct_from_ema_200_52w_high = ((ema_vol_200_latest - ema_vol_200_52w_high) / ema_vol_200_52w_high) * 100 if ema_vol_200_52w_high > 0 else 0

        # 6. Turnover Moving Averages
        turnover_20 = df['Turnover_Cr'].tail(20).mean()
        turnover_50 = df['Turnover_Cr'].tail(50).mean()
        turnover_100 = df['Turnover_Cr'].tail(100).mean()

        return sym, {
            "30 Days Average Rupee Volume(Cr.)": round(avg_rupee_vol_30, 2),
            "RVOL": round(rvol, 2),
            "Daily Rupee Turnover 20(Cr.)": round(turnover_20, 2),
            "Daily Rupee Turnover 50(Cr.)": round(turnover_50, 2),
            "Daily Rupee Turnover 100(Cr.)": round(turnover_100, 2),
            "200 Days EMA Volume": round(ema_vol_200_latest, 0),
            "% from 52W High 200 Days EMA Volume": round(pct_from_ema_200_52w_high, 2),
            "5 Days MA ADR(%)": round(adr_5, 2),
            "14 Days MA ADR(%)": round(adr_14, 2),
            "20 Days MA ADR(%)": round(adr_20, 2),
            "30 Days MA ADR(%)": round(adr_30, 2),
            "% from ATH": round(pct_from_ath, 2),
            "ATH_Value": round(ath, 2),
            "Gap Up %": round(gap_up_pct, 2),
            "Day Range(%)": round(day_range_pct, 2),
            "6 Month Returns(%)": round(returns_6m, 2),
            "% from 52W Low": round(pct_from_52w_low, 2)
        }
    except Exception as e:
        return sym, None

def main():
    print("Loading base analysis data...")
    try:
        with open(JSON_INPUT, "r") as f:
            base_data = json.load(f)
    except Exception as e:
        print(f"Error: {JSON_INPUT} not found. Run bulk_market_analyzer.py first.")
        return

    print("Loading Price Bands (Circuit Limits)...")
    price_band_map = {}
    try:
        with open(PRICE_BANDS_FILE, "r") as f:
            pb_data = json.load(f)
            for item in pb_data:
                price_band_map[item.get("Symbol")] = item.get("Band")
    except:
        print("Warning: Price bands file not found.")

    print("Processing OHLCV metrics for all stocks...")
    csv_files = glob.glob(os.path.join(OHLCV_DIR, "*.csv"))
    
    advanced_metrics_map = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_symbol_csv, cf) for cf in csv_files]
        for future in futures:
            sym, result = future.result()
            if result:
                advanced_metrics_map[sym] = result

    print(f"Updating {len(base_data)} stocks in master JSON...")
    
    for stock in base_data:
        sym = stock.get("Symbol")
        
        # 1. Update Circuit Limit
        if sym in price_band_map:
            stock["Circuit Limit"] = price_band_map[sym]
        
        # 2. Update Advanced Metrics
        if sym in advanced_metrics_map:
            metrics = advanced_metrics_map[sym]
            
            # --- HYBRID FIX: Eliminate 1-day lag ---
            # Use Live LTP from master_data if available
            live_ltp = pd.to_numeric(stock.get("Ltp"), errors='coerce')
            if pd.notnull(live_ltp) and live_ltp > 0:
                ath = metrics.get("ATH_Value", 0)
                if ath > 0:
                    metrics["% from ATH"] = round(((ath - live_ltp) / ath) * 100, 2)
            
            # Merge and clean up helper
            stock.update(metrics)
            if "ATH_Value" in stock: del stock["ATH_Value"]
        else:
            # Initialize with 0 for consistency if missing
            placeholders = [
                "30 Days Average Rupee Volume(Cr.)", "RVOL", 
                "Daily Rupee Turnover 20(Cr.)", "Daily Rupee Turnover 50(Cr.)", "Daily Rupee Turnover 100(Cr.)",
                "200 Days EMA Volume", "% from 52W High 200 Days EMA Volume", "5 Days MA ADR(%)", 
                "14 Days MA ADR(%)", "20 Days MA ADR(%)", "30 Days MA ADR(%)", "% from ATH", 
                "Gap Up %", "Day Range(%)", "6 Month Returns(%)", "% from 52W Low"
            ]
            for p in placeholders:
                if p not in stock: stock[p] = 0.0

    with open(JSON_OUTPUT, "w") as f:
        json.dump(base_data, f, indent=4)
    
    print(f"Successfully updated master JSON: {JSON_OUTPUT}")

if __name__ == "__main__":
    main()
