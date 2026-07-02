"""Build the base stock analysis artifact from Dhan fundamental and scan data."""

import csv
import os
import sys

from pipeline_utils import BASE_DIR, load_json, save_json


FUNDAMENTAL_FILE = os.path.join(BASE_DIR, "fundamental_data.json")
ADVANCED_FILE = os.path.join(BASE_DIR, "advanced_indicator_data.json")
DHAN_DATA_FILE = os.path.join(BASE_DIR, "dhan_data_response.json")
LISTING_DATES_FILE = os.path.join(BASE_DIR, "nse_equity_list.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")

REQUESTED_INDEX_IDS = {
    13, 51, 38, 17, 18, 19, 20, 37, 1, 442, 443, 22, 5, 3, 444, 7, 14,
    25, 27, 28, 447, 35, 41, 46, 44, 16, 43, 42, 45, 39, 466, 34, 32,
    15, 33, 31, 30, 29,
}


def get_float(value_str):
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return 0.0


def calculate_change(current, previous):
    if previous == 0:
        return 0.0
    return ((current - previous) / abs(previous)) * 100


def calculate_cagr(current, previous, years):
    """Return CAGR only when both endpoints are positive."""
    if current <= 0 or previous <= 0 or years <= 0:
        return 0.0
    return ((current / previous) ** (1 / years) - 1) * 100


def get_value_from_pipe_string(pipe_string, index):
    if not pipe_string:
        return 0.0
    parts = pipe_string.split("|")
    if index < len(parts):
        return get_float(parts[index])
    return 0.0


def load_listing_dates(path=LISTING_DATES_FILE):
    listing_date_map = {}
    try:
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row.get("SYMBOL")
                date_list = row.get(" DATE OF LISTING") or row.get("DATE OF LISTING")
                if symbol and date_list:
                    listing_date_map[symbol] = date_list
        print(f"Loaded listing dates for {len(listing_date_map)} symbols.")
    except FileNotFoundError:
        print("Warning: nse_equity_list.csv not found.")
    return listing_date_map


def map_scan_rows_by_symbol(path, symbol_key, label, missing_warning):
    mapped = {}
    try:
        for item in load_json(path):
            symbol = item.get(symbol_key)
            if symbol:
                mapped[symbol] = item
        print(f"Loaded {label} for {len(mapped)} symbols.")
    except FileNotFoundError:
        print(missing_warning)
    return mapped


def quarterly_metric_fields(prefix, source, pipe_name):
    latest = get_value_from_pipe_string(source.get(pipe_name), 0)
    previous = get_value_from_pipe_string(source.get(pipe_name), 1)
    two_back = get_value_from_pipe_string(source.get(pipe_name), 2)
    three_back = get_value_from_pipe_string(source.get(pipe_name), 3)
    last_year = get_value_from_pipe_string(source.get(pipe_name), 4)
    return {
        f"{prefix} Latest Quarter": latest,
        f"{prefix} Previous Quarter": previous,
        f"{prefix} 2 Quarters Back": two_back,
        f"{prefix} 3 Quarters Back": three_back,
        f"{prefix} Last Year Quarter": last_year,
        f"QoQ % {prefix} Latest": round(calculate_change(latest, previous), 2),
        f"YoY % {prefix} Latest": round(calculate_change(latest, last_year), 2),
    }


def valuation_fields(cv, ttm_cy, roce_roe, bs_c, eps_latest, yoy_eps):
    roe = get_float(roce_roe.get("ROE"))
    roce = get_float(roce_roe.get("ROCE"))
    pe = get_float(cv.get("STOCK_PE"))

    non_current_liab = get_value_from_pipe_string(bs_c.get("NON_CURRENT_LIABILITIES"), 0)
    total_equity = get_value_from_pipe_string(bs_c.get("TOTAL_EQUITY"), 0)
    de_ratio = non_current_liab / total_equity if total_equity != 0 else 0.0

    peg = pe / yoy_eps if yoy_eps > 0 and pe > 0 else 0.0

    forward_pe = 0.0
    if eps_latest > 0 and pe > 0:
        annualized_eps = eps_latest * 4
        ttm_eps = get_float(ttm_cy.get("EPS"))
        if annualized_eps > 0:
            forward_pe = pe * (ttm_eps / annualized_eps)

    return {
        "ROE(%)": roe,
        "ROCE(%)": roce,
        "D/E": round(de_ratio, 2),
        "OPM TTM(%)": get_float(ttm_cy.get("OPM")),
        "P/E": pe,
        "PEG": round(peg, 2),
        "Forward P/E": round(forward_pe, 2),
        "Historical P/E 5": 0.0,
    }


def ownership_fields(shp, market_cap_cr, ltp):
    fii_latest = get_value_from_pipe_string(shp.get("FII"), 0)
    fii_prev = get_value_from_pipe_string(shp.get("FII"), 1)
    dii_latest = get_value_from_pipe_string(shp.get("DII"), 0)
    dii_prev = get_value_from_pipe_string(shp.get("DII"), 1)

    promoter_latest = get_value_from_pipe_string(shp.get("PROMOTER"), 0)
    free_float_pct = 100.0 - promoter_latest if shp and promoter_latest is not None and promoter_latest >= 0 else 0.0

    float_shares_cr = 0.0
    if market_cap_cr > 0 and ltp > 0:
        total_shares_cr = market_cap_cr / ltp
        float_shares_cr = total_shares_cr * (free_float_pct / 100.0)

    return {
        "FII % change QoQ": round(fii_latest - fii_prev, 2),
        "DII % change QoQ": round(dii_latest - dii_prev, 2),
        "Free Float(%)": round(free_float_pct, 2),
        "Float Shares(Cr.)": round(float_shares_cr, 2),
    }


def index_memberships(tech):
    indices_found = []
    idx_list_raw = tech.get("idxlist", [])
    if isinstance(idx_list_raw, list):
        for idx_obj in idx_list_raw:
            idx_id = idx_obj.get("Indexid")
            idx_name = idx_obj.get("Name")
            if idx_id in REQUESTED_INDEX_IDS and idx_name:
                indices_found.append(idx_name)
    return ", ".join(indices_found) if indices_found else "N/A"


def average_status(items, suffix, ltp):
    signals = []
    for item in items:
        indicator_name = item.get("Indicator", "").replace(suffix, "")
        value = get_float(item.get("Value"))
        if indicator_name in {"20", "50", "200"} and value > 0 and ltp > 0:
            diff = ((ltp - value) / value) * 100
            status = "Above" if diff > 0 else "Below"
            signals.append(f"{suffix.replace('-', '')} {indicator_name}: {status} ({round(diff, 1)}%)")
    return signals


def technical_sentiment(advanced_tech):
    sentiment_summary = []
    for item in advanced_tech.get("TechnicalIndicators", []):
        name = item.get("Indicator", "")
        action = item.get("Action", "")
        if "RSI" in name:
            sentiment_summary.append(f"RSI: {action}")
        elif "MACD" in name:
            sentiment_summary.append(f"MACD: {action}")
    return " | ".join(sentiment_summary)


def classic_pivot(advanced_tech):
    pivots = advanced_tech.get("Pivots", [])
    if pivots and isinstance(pivots, list):
        return pivots[0].get("Classic", {}).get("PP", "N/A")
    return "N/A"


def analyze_stock(item, tech, advanced_tech, listing_date_map):
    symbol = item.get("Symbol", "UNKNOWN")
    cq = item.get("incomeStat_cq", {})
    cy = item.get("incomeStat_cy", {})
    ttm_cy = item.get("TTM_cy", {})
    cv = item.get("CV", {})
    roce_roe = item.get("roce_roe", {})
    shp = item.get("sHp", {})
    bs_c = item.get("bs_c", {})

    industry = cv.get("INDUSTRY_NAME", "N/A")
    sector = cv.get("SECTOR", "N/A")
    market_cap_cr = get_float(cv.get("MARKET_CAP"))
    ltp = get_float(tech.get("Ltp", 0))

    net_profit = quarterly_metric_fields("Net Profit", cq, "NET_PROFIT")
    eps = quarterly_metric_fields("EPS", cq, "EPS")
    sales = quarterly_metric_fields("Sales", cq, "SALES")
    opm = quarterly_metric_fields("OPM", cq, "OPM")

    sales_current_annual = get_value_from_pipe_string(cy.get("SALES"), 0)
    sales_5_years_ago = get_value_from_pipe_string(cy.get("SALES"), 5)

    high_52w = get_float(tech.get("High1Yr", 0))
    pct_from_52w_high = ((ltp - high_52w) / high_52w) * 100 if high_52w > 0 and ltp > 0 else 0.0

    stock_analysis = {
        "Symbol": symbol,
        "Name": item.get("Name", ""),
        "Listing Date": listing_date_map.get(symbol, "N/A"),
        "Basic Industry": industry,
        "Sector": sector,
        "Market Cap(Cr.)": market_cap_cr,
        "Latest Quarter": cq.get("YEAR", "").split("|")[0] if cq.get("YEAR") else "N/A",
        **net_profit,
        **eps,
        "EPS Last Year": get_value_from_pipe_string(cy.get("EPS"), 0),
        "EPS 2 Years Back": get_value_from_pipe_string(cy.get("EPS"), 1),
        **sales,
        "Sales Growth 5 Years(%)": round(calculate_cagr(sales_current_annual, sales_5_years_ago, 5), 2),
        **opm,
        **valuation_fields(cv, ttm_cy, roce_roe, bs_c, eps["EPS Latest Quarter"], eps["YoY % EPS Latest"]),
        **ownership_fields(shp, market_cap_cr, ltp),
        "% from 52W High": round(pct_from_52w_high, 2),
    }

    rsi_14 = get_float(tech.get("DayRSI14CurrentCandle", 0))
    sma_signals = average_status(advanced_tech.get("SMA", []), "-SMA", ltp)
    ema_signals = average_status(advanced_tech.get("EMA", []), "-EMA", ltp)

    stock_analysis.update(
        {
            "Stock Price(₹)": ltp,
            "Index": index_memberships(tech),
            "1 Day Returns(%)": get_float(tech.get("PPerchange", 0)),
            "1 Week Returns(%)": get_float(tech.get("PricePerchng1week", 0)),
            "1 Month Returns(%)": get_float(tech.get("PricePerchng1mon", 0)),
            "3 Month Returns(%)": get_float(tech.get("PricePerchng3mon", 0)),
            "1 Year Returns(%)": get_float(tech.get("PricePerchng1year", 0)),
            "RSI (14)": round(rsi_14, 2),
            "Gap Up %": 0.0,
            "SMA Status": " | ".join(sma_signals),
            "EMA Status": " | ".join(ema_signals),
            "Technical Sentiment": technical_sentiment(advanced_tech),
            "Pivot Point": classic_pivot(advanced_tech),
        }
    )
    return stock_analysis


def analyze_all_stocks():
    print("Loading fundamental data...")
    try:
        data = load_json(FUNDAMENTAL_FILE)
    except FileNotFoundError:
        print(f"Error: {FUNDAMENTAL_FILE} not found.")
        return False

    listing_date_map = load_listing_dates()
    dhan_tech_map = map_scan_rows_by_symbol(DHAN_DATA_FILE, "Sym", "technical data", f"Warning: {DHAN_DATA_FILE} not found.")
    advanced_tech_map = map_scan_rows_by_symbol(
        ADVANCED_FILE,
        "Symbol",
        "advanced indicators",
        f"Warning: {ADVANCED_FILE} not found. Running without advanced indicators.",
    )

    print(f"Analyzing {len(data)} stocks...")
    final_data = [
        analyze_stock(
            item,
            dhan_tech_map.get(item.get("Symbol", "UNKNOWN"), {}),
            advanced_tech_map.get(item.get("Symbol", "UNKNOWN"), {}),
            listing_date_map,
        )
        for item in data
    ]

    save_json(OUTPUT_FILE, final_data)
    print(f"Successfully saved analysis for {len(final_data)} stocks (filtered from {len(final_data)}) to {OUTPUT_FILE}")
    return True


if __name__ == "__main__":
    sys.exit(0 if analyze_all_stocks() else 1)
