"""Build the base stock analysis artifact from Dhan fundamental and scan data."""

import csv
import os
import sys
import math

from pipeline_utils import BASE_DIR, load_json, save_json


FUNDAMENTAL_FILE = os.path.join(BASE_DIR, "fundamental_data.json")
ADVANCED_FILE = os.path.join(BASE_DIR, "advanced_indicator_data.json")
DHAN_DATA_FILE = os.path.join(BASE_DIR, "dhan_data_response.json")
SME_DATA_FILE = os.path.join(BASE_DIR, "sme_market_data.json")
LISTING_DATES_FILE = os.path.join(BASE_DIR, "nse_equity_list.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")

def get_float(value_str):
    return get_optional_float(value_str)


def get_optional_float(value):
    if value is None or value == "":
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, TypeError):
        return None


def calculate_change(current, previous):
    if current is None or previous in (None, 0):
        return None
    return ((current - previous) / abs(previous)) * 100


def calculate_cagr(current, previous, years):
    """Return CAGR only when both endpoints are positive."""
    if current is None or previous is None or current <= 0 or previous <= 0 or years <= 0:
        return None
    return ((current / previous) ** (1 / years) - 1) * 100


def get_value_from_pipe_string(pipe_string, index):
    if not pipe_string:
        return None
    parts = pipe_string.split("|")
    if index < len(parts):
        return get_float(parts[index])
    return None


def rounded(value, digits=2):
    return round(value, digits) if value is not None and math.isfinite(value) else None


def positive(value):
    return value is not None and value > 0


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


def load_sme_map(path=SME_DATA_FILE):
    try:
        rows = load_json(path)
    except FileNotFoundError:
        print(f"Warning: {path} not found. SME classification unavailable.")
        return None
    return {row.get("Symbol"): row for row in rows if row.get("Symbol")}


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
        f"QoQ % {prefix} Latest": rounded(calculate_change(latest, previous)),
        f"YoY % {prefix} Latest": rounded(calculate_change(latest, last_year)),
    }


def valuation_fields(cv, ttm_cy, roce_roe, bs_c, eps_latest, yoy_eps):
    roe = get_float(roce_roe.get("ROE"))
    roce = get_float(roce_roe.get("ROCE"))
    pe = get_float(cv.get("STOCK_PE"))

    non_current_liab = get_value_from_pipe_string(bs_c.get("NON_CURRENT_LIABILITIES"), 0)
    total_equity = get_value_from_pipe_string(bs_c.get("TOTAL_EQUITY"), 0)
    de_ratio = non_current_liab / total_equity if non_current_liab is not None and total_equity not in (None, 0) else None

    peg = pe / yoy_eps if positive(yoy_eps) and positive(pe) else None

    forward_pe = None
    if positive(eps_latest) and positive(pe):
        annualized_eps = eps_latest * 4
        ttm_eps = get_float(ttm_cy.get("EPS"))
        if ttm_eps is not None:
            forward_pe = pe * (ttm_eps / annualized_eps)

    return {
        "ROE(%)": roe,
        "ROCE(%)": roce,
        "D/E": rounded(de_ratio),
        "OPM TTM(%)": get_float(ttm_cy.get("OPM")),
        "P/E": pe,
        "PEG": rounded(peg),
        "Forward P/E": rounded(forward_pe),
        "Historical P/E 5": None,
    }


def ownership_fields(shp, market_cap_cr, ltp, total_shares):
    fii_latest = get_value_from_pipe_string(shp.get("FII"), 0)
    fii_prev = get_value_from_pipe_string(shp.get("FII"), 1)
    dii_latest = get_value_from_pipe_string(shp.get("DII"), 0)
    dii_prev = get_value_from_pipe_string(shp.get("DII"), 1)

    promoter_history = shp.get("PROMOTER")
    promoter_latest = get_value_from_pipe_string(promoter_history, 0) if promoter_history else None
    free_float_pct = 100.0 - promoter_latest if promoter_latest is not None and promoter_latest >= 0 else None

    total_shares_cr = total_shares / 10_000_000 if positive(total_shares) else None
    if total_shares_cr is None and positive(market_cap_cr) and positive(ltp):
        total_shares_cr = market_cap_cr / ltp
    float_shares_cr = total_shares_cr * (free_float_pct / 100.0) if free_float_pct is not None and total_shares_cr is not None else None

    return {
        "FII % change QoQ": rounded(fii_latest - fii_prev) if None not in (fii_latest, fii_prev) else None,
        "DII % change QoQ": rounded(dii_latest - dii_prev) if None not in (dii_latest, dii_prev) else None,
        "Free Float(%)": round(free_float_pct, 2) if free_float_pct is not None else None,
        "Float Shares(Cr.)": round(float_shares_cr, 2) if float_shares_cr is not None else None,
    }


def index_memberships(tech):
    """Preserve every current provider membership; historical dates are not implied."""
    indices_found = []
    idx_list_raw = tech.get("idxlist", [])
    if isinstance(idx_list_raw, list):
        for idx_obj in idx_list_raw:
            idx_name = idx_obj.get("Name")
            if idx_name:
                indices_found.append(idx_name)
    return sorted(set(indices_found))


def average_status(items, suffix, ltp):
    signals = []
    for item in items:
        indicator_name = item.get("Indicator", "").replace(suffix, "")
        value = get_float(item.get("Value"))
        if indicator_name in {"20", "50", "200"} and positive(value) and positive(ltp):
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


def analyze_stock(item, tech, advanced_tech, listing_date_map, sme_map=None):
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
    market_cap_cr = get_float(tech.get("Mcap") or cv.get("MARKET_CAP"))
    ltp = get_float(tech.get("Ltp"))
    total_shares = get_optional_float(tech.get("TotalShares")) or 0.0
    volume = get_optional_float(tech.get("Volume", tech.get("volume")))
    sme_record = sme_map.get(symbol) if sme_map is not None else None

    net_profit = quarterly_metric_fields("Net Profit", cq, "NET_PROFIT")
    eps = quarterly_metric_fields("EPS", cq, "EPS")
    sales = quarterly_metric_fields("Sales", cq, "SALES")
    opm = quarterly_metric_fields("OPM", cq, "OPM")

    sales_current_annual = get_value_from_pipe_string(cy.get("SALES"), 0)
    sales_5_years_ago = get_value_from_pipe_string(cy.get("SALES"), 5)

    high_52w = get_float(tech.get("High1Yr"))
    pct_from_52w_high = ((ltp - high_52w) / high_52w) * 100 if positive(high_52w) and positive(ltp) else None

    ownership = ownership_fields(shp, market_cap_cr, ltp, total_shares)
    free_float_pct = ownership["Free Float(%)"]

    stock_analysis = {
        "Symbol": symbol,
        "Name": item.get("Name", ""),
        "Listing Date": listing_date_map.get(symbol, "N/A"),
        "ISIN": item.get("ISIN") or item.get("isin"),
        "Security ID": item.get("Sid") or item.get("security_id"),
        "Basic Industry": industry,
        "Sector": sector,
        "Market Cap(Cr.)": market_cap_cr,
        "Latest Quarter": cq.get("YEAR", "").split("|")[0] if cq.get("YEAR") else "N/A",
        **net_profit,
        **eps,
        "EPS Last Year": get_value_from_pipe_string(cy.get("EPS"), 0),
        "EPS 2 Years Back": get_value_from_pipe_string(cy.get("EPS"), 1),
        **sales,
        "Sales Growth 5 Years(%)": rounded(calculate_cagr(sales_current_annual, sales_5_years_ago, 5)),
        **opm,
        **valuation_fields(cv, ttm_cy, roce_roe, bs_c, eps["EPS Latest Quarter"], eps["YoY % EPS Latest"]),
        **ownership,
        "% from 52W High": rounded(pct_from_52w_high),
    }

    rsi_14 = get_float(tech.get("DayRSI14CurrentCandle"))
    sma_signals = average_status(advanced_tech.get("SMA", []), "-SMA", ltp)
    ema_signals = average_status(advanced_tech.get("EMA", []), "-EMA", ltp)

    stock_analysis.update(
        {
            "scanner_schema_version": "2.0",
            "Stock Price(₹)": ltp,
            "exchange": tech.get("Exch", "NSE"),
            "instrument": tech.get("Inst", "EQUITY"),
            "segment": tech.get("Seg", "E"),
            "listing_board": "SME" if sme_record else "MAINBOARD" if sme_map is not None else "UNKNOWN",
            "is_sme": True if sme_record else False if sme_map is not None else None,
            "listing_series": sme_record.get("Series") if sme_record else None,
            "close": ltp,
            "open": get_optional_float(tech.get("Open")),
            "high": get_optional_float(tech.get("High")),
            "low": get_optional_float(tech.get("Low")),
            "volume": volume,
            "rupee_volume": round(ltp * volume, 2) if positive(ltp) and volume is not None else None,
            "change_percent": get_optional_float(tech.get("PPerchange")),
            "market_cap_crore": market_cap_cr,
            "shares_outstanding": int(total_shares) if total_shares > 0 else None,
            "share_capital": get_float(tech.get("ShareCapital", 0)) or None,
            "sector": tech.get("Sector") or sector,
            "industry": industry,
            "free_float_percent": free_float_pct,
            "float_shares": round(total_shares * (free_float_pct / 100.0))
            if total_shares > 0 and free_float_pct is not None else None,
            "perf_1w": get_optional_float(tech.get("PricePerchng1week")),
            "perf_1m": get_optional_float(tech.get("PricePerchng1mon")),
            "perf_3m": get_optional_float(tech.get("PricePerchng3mon")),
            "perf_6m": get_optional_float(tech.get("PricePerchng6mon")),
            "perf_12m": get_optional_float(tech.get("PricePerchng1year")),
            "sma10": get_optional_float(tech.get("DaySMA10CurrentCandle")),
            "sma20": get_optional_float(tech.get("DaySMA20CurrentCandle")),
            "sma50": get_optional_float(tech.get("DaySMA50CurrentCandle")),
            "sma200": get_optional_float(tech.get("DaySMA200CurrentCandle")),
            "rsi14": rounded(rsi_14),
            "Index": ", ".join(index_memberships(tech)) or "N/A",
            "Index Memberships": index_memberships(tech),
            "Index Membership As Of": "current_snapshot",
            "1 Day Returns(%)": get_float(tech.get("PPerchange")),
            "1 Week Returns(%)": get_float(tech.get("PricePerchng1week")),
            "1 Month Returns(%)": get_float(tech.get("PricePerchng1mon")),
            "3 Month Returns(%)": get_float(tech.get("PricePerchng3mon")),
            "1 Year Returns(%)": get_float(tech.get("PricePerchng1year")),
            "RSI (14)": rounded(rsi_14),
            "Gap Up %": None,
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
    sme_map = load_sme_map()
    dhan_tech_map = map_scan_rows_by_symbol(DHAN_DATA_FILE, "Sym", "technical data", f"Warning: {DHAN_DATA_FILE} not found.")
    advanced_tech_map = map_scan_rows_by_symbol(
        ADVANCED_FILE,
        "Symbol",
        "advanced indicators",
        f"Warning: {ADVANCED_FILE} not found. Running without advanced indicators.",
    )

    # Missing fundamental responses must not silently remove a security.
    fundamental_map = {item['Symbol']: item for item in data if item.get('Symbol')}
    master = load_json(os.path.join(BASE_DIR, 'master_isin_map.json'))
    data = [{**item, **fundamental_map.get(item['Symbol'], {})} for item in master]
    print(f"Analyzing {len(data)} stocks...")
    final_data = [
        analyze_stock(
            item,
            dhan_tech_map.get(item.get("Symbol", "UNKNOWN"), {}),
            advanced_tech_map.get(item.get("Symbol", "UNKNOWN"), {}),
            listing_date_map,
            sme_map,
        )
        for item in data
    ]

    save_json(OUTPUT_FILE, final_data)
    print(f"Successfully saved analysis for {len(final_data)} stocks (filtered from {len(final_data)}) to {OUTPUT_FILE}")
    return True


if __name__ == "__main__":
    sys.exit(0 if analyze_all_stocks() else 1)
