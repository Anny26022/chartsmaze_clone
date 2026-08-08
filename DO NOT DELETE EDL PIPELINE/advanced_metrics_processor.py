import pandas as pd
import os
import glob
import sys
from concurrent.futures import ThreadPoolExecutor

from pipeline_utils import BASE_DIR, load_json, save_json

# --- Configuration ---
JSON_INPUT = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
PRICE_BANDS_FILE = os.path.join(BASE_DIR, "complete_price_bands.json")
OHLCV_DIR = os.path.join(BASE_DIR, "ohlcv_data")
JSON_OUTPUT = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")

SCANNER_DERIVED_FIELDS = [
    'as_of_date', 'gap_percent', 'range_percent', 'avg_volume_20',
    'avg_rupee_volume_20', 'relative_volume_20', 'atr14', 'atr_percent_14',
    'adr20', 'adr_percent_20', 'close_above_sma10', 'close_above_sma20',
    'close_above_sma50', 'close_above_sma200', 'sma10_above_sma20',
    'sma20_above_sma50', 'sma50_above_sma200', 'bullish_candle',
    'close_near_day_high', 'breakout_above_20d_high', 'breakout_above_50d_high',
    'near_52w_high', 'breakout_above_52w_high', 'is_nr7', 'is_inside_day',
    'is_bullish_engulfing', 'distance_from_sma20_percent',
    'distance_from_sma50_percent', 'distance_from_sma200_percent',
    'distance_from_52w_high_percent', 'distance_from_52w_low_percent',
    'sma50_crossed_above_sma200_today',
]

def calculate_ema(series, periods):
    return series.ewm(span=periods, adjust=False).mean()


def value_or_none(value, digits=2):
    if pd.isna(value):
        return None
    return round(float(value), digits)


def boolean_or_none(condition, available):
    return bool(condition) if available else None


def drop_copied_live_snapshot(df):
    """Drop a non-trading-day snapshot copied verbatim from the prior session."""
    columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    if len(df) > 1 and df[columns].iloc[-1].equals(df[columns].iloc[-2]):
        return df.iloc[:-1].copy()
    return df


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

        df = df.sort_values('Date') if 'Date' in df.columns else df
        df = drop_copied_live_snapshot(df)
        if len(df) < 5:
            return sym, None

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

        # 7. Normalized scanner fields. Values are null when there is not enough
        # history to calculate a trustworthy metric.
        close = float(latest['Close'])
        high = float(latest['High'])
        low = float(latest['Low'])
        open_price = float(latest['Open'])
        volume = float(latest['Volume'])
        prior_20 = df.iloc[-21:-1] if len(df) >= 21 else pd.DataFrame()

        sma_series = {
            period: df['Close'].rolling(period, min_periods=period).mean()
            for period in (10, 20, 50, 200)
        }
        rolling_sma = {
            period: series.iloc[-1] if len(df) >= period else None
            for period, series in sma_series.items()
        }
        previous_sma = {
            period: series.iloc[-2] if len(df) >= period + 1 else None
            for period, series in sma_series.items()
        }

        true_range = pd.concat([
            df['High'] - df['Low'],
            (df['High'] - df['Close'].shift(1)).abs(),
            (df['Low'] - df['Close'].shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr14 = true_range.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean().iloc[-1] if len(df) >= 14 else None
        adr20 = (df['High'] - df['Low']).tail(20).mean() if len(df) >= 20 else None
        adr_percent_20 = df['Daily_Range_Pct'].tail(20).mean() if len(df) >= 20 else None
        avg_volume_20 = prior_20['Volume'].mean() if len(prior_20) == 20 else None
        avg_rupee_volume_20 = (prior_20['Close'] * prior_20['Volume']).mean() if len(prior_20) == 20 else None

        prior_20_high = df['High'].iloc[-21:-1].max() if len(df) >= 21 else None
        prior_50_high = df['High'].iloc[-51:-1].max() if len(df) >= 51 else None
        prior_52w_high = df['High'].iloc[-253:-1].max() if len(df) >= 253 else None
        current_range = high - low
        prior_six_ranges = (df['High'] - df['Low']).iloc[-7:-1] if len(df) >= 7 else pd.Series(dtype=float)

        scanner_metrics = {
            'as_of_date': str(latest['Date']) if 'Date' in df.columns else None,
            'rupee_volume': value_or_none(close * volume),
            'gap_percent': value_or_none(gap_up_pct),
            'range_percent': value_or_none(day_range_pct),
            'avg_volume_20': value_or_none(avg_volume_20, 0),
            'avg_rupee_volume_20': value_or_none(avg_rupee_volume_20),
            'relative_volume_20': value_or_none(volume / avg_volume_20) if avg_volume_20 and avg_volume_20 > 0 else None,
            'atr14': value_or_none(atr14),
            'atr_percent_14': value_or_none((atr14 / close) * 100) if atr14 is not None and close > 0 else None,
            'adr20': value_or_none(adr20),
            'adr_percent_20': value_or_none(adr_percent_20),
            'close_above_sma10': boolean_or_none(close > rolling_sma[10], rolling_sma[10] is not None),
            'close_above_sma20': boolean_or_none(close > rolling_sma[20], rolling_sma[20] is not None),
            'close_above_sma50': boolean_or_none(close > rolling_sma[50], rolling_sma[50] is not None),
            'close_above_sma200': boolean_or_none(close > rolling_sma[200], rolling_sma[200] is not None),
            'sma10_above_sma20': boolean_or_none(rolling_sma[10] > rolling_sma[20], rolling_sma[10] is not None and rolling_sma[20] is not None),
            'sma20_above_sma50': boolean_or_none(rolling_sma[20] > rolling_sma[50], rolling_sma[20] is not None and rolling_sma[50] is not None),
            'sma50_above_sma200': boolean_or_none(rolling_sma[50] > rolling_sma[200], rolling_sma[50] is not None and rolling_sma[200] is not None),
            'sma50_crossed_above_sma200_today': boolean_or_none(
                previous_sma[50] <= previous_sma[200] and rolling_sma[50] > rolling_sma[200],
                previous_sma[50] is not None and previous_sma[200] is not None,
            ),
            'distance_from_sma20_percent': value_or_none(((close - rolling_sma[20]) / rolling_sma[20]) * 100) if rolling_sma[20] else None,
            'distance_from_sma50_percent': value_or_none(((close - rolling_sma[50]) / rolling_sma[50]) * 100) if rolling_sma[50] else None,
            'distance_from_sma200_percent': value_or_none(((close - rolling_sma[200]) / rolling_sma[200]) * 100) if rolling_sma[200] else None,
            'distance_from_52w_high_percent': value_or_none(((close - prior_52w_high) / prior_52w_high) * 100) if prior_52w_high else None,
            'distance_from_52w_low_percent': value_or_none(pct_from_52w_low),
            'bullish_candle': close > open_price,
            'close_near_day_high': boolean_or_none(
                (high - close) / current_range <= 0.25,
                current_range > 0,
            ),
            'breakout_above_20d_high': boolean_or_none(close > prior_20_high, prior_20_high is not None),
            'breakout_above_50d_high': boolean_or_none(close > prior_50_high, prior_50_high is not None),
            'near_52w_high': boolean_or_none(close >= prior_52w_high * 0.95, prior_52w_high is not None),
            'breakout_above_52w_high': boolean_or_none(close > prior_52w_high, prior_52w_high is not None),
            'is_nr7': boolean_or_none(current_range <= prior_six_ranges.min(), len(prior_six_ranges) == 6),
            'is_inside_day': boolean_or_none(high <= float(prev['High']) and low >= float(prev['Low']), len(df) >= 2),
            'is_bullish_engulfing': boolean_or_none(
                close > open_price and float(prev['Close']) < float(prev['Open'])
                and open_price <= float(prev['Close']) and close >= float(prev['Open']),
                len(df) >= 2,
            ),
        }
        scanner_metrics.update({
            f'sma{period}': value_or_none(value)
            for period, value in rolling_sma.items() if value is not None
        })

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
            "% from 52W Low": round(pct_from_52w_low, 2),
            **scanner_metrics,
        }
    except Exception as e:
        return sym, None

def main():
    print("Loading base analysis data...")
    try:
        base_data = load_json(JSON_INPUT)
    except Exception as e:
        print(f"Error: {JSON_INPUT} not found. Run bulk_market_analyzer.py first.")
        return False

    print("Loading Price Bands (Circuit Limits)...")
    price_band_map = {}
    try:
        for item in load_json(PRICE_BANDS_FILE):
            price_band_map[item.get("Symbol")] = item.get("Band")
    except Exception:
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
            live_ltp = pd.to_numeric(stock.get("close", stock.get("Stock Price(₹)")), errors='coerce')
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

        for field in SCANNER_DERIVED_FIELDS:
            stock.setdefault(field, None)

    save_json(JSON_OUTPUT, base_data)
    
    print(f"Successfully updated master JSON: {JSON_OUTPUT}")
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
