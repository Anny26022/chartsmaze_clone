import json
import csv
import os

def get_float(value_str):
    try:
        return float(value_str)
    except (ValueError, TypeError):
        return 0.0

def calculate_change(current, previous):
    if previous == 0:
        return 0.0
    return ((current - previous) / abs(previous)) * 100

def get_value_from_pipe_string(pipe_string, index):
    if not pipe_string:
        return 0.0
    parts = pipe_string.split('|')
    if index < len(parts):
        return get_float(parts[index])
    return 0.0

def analyze_all_stocks():
    # Paths relative to script
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    # BASE_DIR is defined globally
    input_file = os.path.join(BASE_DIR, "fundamental_data.json")
    # output_file is defined globally as OUTPUT_FILE
    ADVANCED_FILE = os.path.join(BASE_DIR, "advanced_indicator_data.json")
    output_file = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")

    print("Loading fundamental data...")
    try:
        with open(input_file, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return

    # Load Listing Dates from NSE CSV
    listing_date_map = {}
    csv_path = os.path.join(BASE_DIR, "nse_equity_list.csv")
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = row.get("SYMBOL")
                date_list = row.get(" DATE OF LISTING") or row.get("DATE OF LISTING")
                if sym and date_list:
                    listing_date_map[sym] = date_list
        print(f"Loaded listing dates for {len(listing_date_map)} symbols.")
    except FileNotFoundError:
        print("Warning: nse_equity_list.csv not found.")

    # Load Technical Data from Dhan Response
    dhan_tech_map = {}
    dhan_file = os.path.join(BASE_DIR, "dhan_data_response.json")
    try:
        with open(dhan_file, "r") as f:
            dhan_data = json.load(f)
            for item in dhan_data:
                sym = item.get("Sym")
                if sym:
                    dhan_tech_map[sym] = item
        print(f"Loaded technical data for {len(dhan_tech_map)} symbols.")
    except FileNotFoundError:
        print(f"Warning: {dhan_file} not found.")

    # Load Advanced Indicator Data
    adv_tech_map = {}
    try:
        with open(ADVANCED_FILE, "r") as f:
            adv_data = json.load(f)
            for item in adv_data:
                sym = item.get("Symbol")
                if sym:
                    adv_tech_map[sym] = item
        print(f"Loaded advanced indicators for {len(adv_tech_map)} symbols.")
    except FileNotFoundError:
        print(f"Warning: {ADVANCED_FILE} not found. Running without advanced indicators.")

    analyzed_data = []
    total_stocks = len(data)
    print(f"Analyzing {total_stocks} stocks...")

    for item in data:
        symbol = item.get("Symbol", "UNKNOWN")
        tech = dhan_tech_map.get(symbol, {})
        adv_tech = adv_tech_map.get(symbol, {})
        
        # --- Extract Data Sections ---
        cq = item.get("incomeStat_cq", {})
        cy = item.get("incomeStat_cy", {})
        ttm_cy = item.get("TTM_cy", {})
        cv = item.get("CV", {})
        roce_roe = item.get("roce_roe", {})
        shp = item.get("sHp", {})
        bs_c = item.get("bs_c", {})

        # --- Base Data ---
        industry = cv.get("INDUSTRY_NAME", "N/A")
        sector = cv.get("SECTOR", "N/A")
        mcap_cr = get_float(cv.get("MARKET_CAP"))
        ltp = get_float(tech.get("Ltp", 0))

        # --- 1. Profit & Loss ---
        np_latest = get_value_from_pipe_string(cq.get("NET_PROFIT"), 0)
        np_prev = get_value_from_pipe_string(cq.get("NET_PROFIT"), 1)
        np_2_back = get_value_from_pipe_string(cq.get("NET_PROFIT"), 2)
        np_3_back = get_value_from_pipe_string(cq.get("NET_PROFIT"), 3)
        np_last_year_q = get_value_from_pipe_string(cq.get("NET_PROFIT"), 4)
        
        qoq_np = calculate_change(np_latest, np_prev)
        yoy_np = calculate_change(np_latest, np_last_year_q)

        # --- 2. EPS ---
        eps_latest = get_value_from_pipe_string(cq.get("EPS"), 0)
        eps_prev = get_value_from_pipe_string(cq.get("EPS"), 1)
        eps_2_back = get_value_from_pipe_string(cq.get("EPS"), 2)
        eps_3_back = get_value_from_pipe_string(cq.get("EPS"), 3)
        eps_last_year_q = get_value_from_pipe_string(cq.get("EPS"), 4)
        eps_last_year_annual = get_value_from_pipe_string(cy.get("EPS"), 0)
        eps_2_years_back_annual = get_value_from_pipe_string(cy.get("EPS"), 1)

        qoq_eps = calculate_change(eps_latest, eps_prev)
        yoy_eps = calculate_change(eps_latest, eps_last_year_q)

        # --- 3. Sales ---
        sales_latest = get_value_from_pipe_string(cq.get("SALES"), 0)
        sales_prev = get_value_from_pipe_string(cq.get("SALES"), 1)
        sales_2_back = get_value_from_pipe_string(cq.get("SALES"), 2)
        sales_3_back = get_value_from_pipe_string(cq.get("SALES"), 3)
        sales_last_year_q = get_value_from_pipe_string(cq.get("SALES"), 4)
        sales_5_years_ago = get_value_from_pipe_string(cy.get("SALES"), 5)
        sales_current_annual = get_value_from_pipe_string(cy.get("SALES"), 0)

        qoq_sales = calculate_change(sales_latest, sales_prev)
        yoy_sales = calculate_change(sales_latest, sales_last_year_q)
        
        sales_growth_5y = 0.0
        if sales_5_years_ago > 0:
            sales_growth_5y = ((sales_current_annual / sales_5_years_ago) ** (1/5) - 1) * 100

        # --- 4. OPM ---
        opm_latest = get_value_from_pipe_string(cq.get("OPM"), 0)
        opm_prev = get_value_from_pipe_string(cq.get("OPM"), 1)
        opm_2_back = get_value_from_pipe_string(cq.get("OPM"), 2)
        opm_3_back = get_value_from_pipe_string(cq.get("OPM"), 3)
        opm_last_year_q = get_value_from_pipe_string(cq.get("OPM"), 4)
        opm_ttm = get_float(ttm_cy.get("OPM"))

        qoq_opm = calculate_change(opm_latest, opm_prev)
        yoy_opm = calculate_change(opm_latest, opm_last_year_q)

        # --- 5. Ratios ---
        roe = get_float(roce_roe.get("ROE"))
        roce = get_float(roce_roe.get("ROCE"))
        pe = get_float(cv.get("STOCK_PE"))
        
        non_current_liab = get_value_from_pipe_string(bs_c.get("NON_CURRENT_LIABILITIES"), 0)
        total_equity = get_value_from_pipe_string(bs_c.get("TOTAL_EQUITY"), 0)
        de_ratio = non_current_liab / total_equity if total_equity != 0 else 0.0

        peg = 0.0
        if yoy_eps > 0 and pe > 0:
            peg = pe / yoy_eps

        forward_pe = 0.0
        if eps_latest > 0 and pe > 0:
            annualized_eps = eps_latest * 4
            ttm_eps = get_float(ttm_cy.get("EPS"))
            if annualized_eps > 0:
                 forward_pe = pe * (ttm_eps / annualized_eps)

        # --- 6. Shareholding & Float ---
        fii_latest = get_value_from_pipe_string(shp.get("FII"), 0)
        fii_prev = get_value_from_pipe_string(shp.get("FII"), 1)
        fii_change_qoq = fii_latest - fii_prev

        dii_latest = get_value_from_pipe_string(shp.get("DII"), 0)
        dii_prev = get_value_from_pipe_string(shp.get("DII"), 1)
        dii_change_qoq = dii_latest - dii_prev
        
        promoter_latest = get_value_from_pipe_string(shp.get("PROMOTER"), 0)
        free_float_pct = 100.0 - promoter_latest if shp and promoter_latest \
            is not None and promoter_latest >= 0 else 0.0
        
        float_shares_cr = 0.0
        if mcap_cr > 0 and ltp > 0:
            total_shares_cr = mcap_cr / ltp
            float_shares_cr = total_shares_cr * (free_float_pct / 100.0)

        # --- 7. Assembly ---
        latest_quarter_str = cq.get("YEAR", "").split('|')[0] if cq.get("YEAR") else "N/A"
        listing_date = listing_date_map.get(symbol, "N/A")
        
        high_52w = get_float(tech.get("High1Yr", 0))
        pct_from_52w_high = 0.0
        if high_52w > 0 and ltp > 0:
            pct_from_52w_high = ((ltp - high_52w) / high_52w) * 100

        stock_analysis = {
            "Symbol": symbol,
            "Name": item.get("Name", ""),
            "Listing Date": listing_date,
            "Basic Industry": industry,
            "Sector": sector,
            "Market Cap(Cr.)": mcap_cr,
            "Latest Quarter": latest_quarter_str,
            "Net Profit Latest Quarter": np_latest,
            "Net Profit Previous Quarter": np_prev,
            "Net Profit 2 Quarters Back": np_2_back,
            "Net Profit 3 Quarters Back": np_3_back,
            "Net Profit Last Year Quarter": np_last_year_q,
            "QoQ % Net Profit Latest": round(qoq_np, 2),
            "YoY % Net Profit Latest": round(yoy_np, 2),
            "EPS Latest Quarter": eps_latest,
            "EPS Previous Quarter": eps_prev,
            "EPS 2 Quarters Back": eps_2_back,
            "EPS 3 Quarters Back": eps_3_back,
            "EPS Last Year Quarter": eps_last_year_q,
            "QoQ % EPS Latest": round(qoq_eps, 2),
            "YoY % EPS Latest": round(yoy_eps, 2),
            "EPS Last Year": eps_last_year_annual,
            "EPS 2 Years Back": eps_2_years_back_annual,
            "Sales Latest Quarter": sales_latest,
            "Sales Previous Quarter": sales_prev,
            "Sales 2 Quarters Back": sales_2_back,
            "Sales 3 Quarters Back": sales_3_back,
            "Sales Last Year Quarter": sales_last_year_q,
            "QoQ % Sales Latest": round(qoq_sales, 2),
            "YoY % Sales Latest": round(yoy_sales, 2),
            "Sales Growth 5 Years(%)": round(sales_growth_5y, 2),
            "OPM Latest Quarter": opm_latest,
            "OPM Previous Quarter": opm_prev,
            "OPM 2 Quarters Back": opm_2_back,
            "OPM 3 Quarters Back": opm_3_back,
            "OPM Last Year Quarter": opm_last_year_q,
            "QoQ % OPM Latest": round(qoq_opm, 2),
            "YoY % OPM Latest": round(yoy_opm, 2),
            "ROE(%)": roe,
            "ROCE(%)": roce,
            "D/E": round(de_ratio, 2),
            "OPM TTM(%)": opm_ttm,
            "P/E": pe,
            "FII % change QoQ": round(fii_change_qoq, 2),
            "DII % change QoQ": round(dii_change_qoq, 2),
            "Free Float(%)": round(free_float_pct, 2),
            "Float Shares(Cr.)": round(float_shares_cr, 2),
            "PEG": round(peg, 2),
            "Forward P/E": round(forward_pe, 2),
            "Historical P/E 5": 0.0,
            "% from 52W High": round(pct_from_52w_high, 2)
        }

        # Technicals from dhan_tech_map
        ltp = get_float(tech.get("Ltp", 0))
        sma_200 = get_float(tech.get("DaySMA200CurrentCandle", 0))
        sma_50 = get_float(tech.get("DaySMA50CurrentCandle", 0))
        rsi_14 = get_float(tech.get("DayRSI14CurrentCandle", 0))
        
        pct_from_sma_200 = 0.0
        if sma_200 > 0 and ltp > 0:
            pct_from_sma_200 = ((ltp - sma_200) / sma_200) * 100
        
        pct_from_sma_50 = 0.0
        if sma_50 > 0 and ltp > 0:
            pct_from_sma_50 = ((ltp - sma_50) / sma_50) * 100
            
        # Index Mapping
        # User requested indices: 13,51,38,17,18,19,20,37,1,442,443,22,5,3,444,7,14,25,27,28,447,35,41,46,44,16,43,42,45,39,466,34,32,15,33,31,30,29
        requested_indices = {13,51,38,17,18,19,20,37,1,442,443,22,5,3,444,7,14,25,27,28,447,35,41,46,44,16,43,42,45,39,466,34,32,15,33,31,30,29}
        indices_found = []
        idx_list_raw = tech.get("idxlist", [])
        if isinstance(idx_list_raw, list):
            for idx_obj in idx_list_raw:
                idx_id = idx_obj.get("Indexid")
                idx_name = idx_obj.get("Name")
                if idx_id in requested_indices and idx_name:
                    indices_found.append(idx_name)
        
        # --- Advanced Indicators (Signals, Pivots, Sentiment) ---
        # Parse SMA Signals - Calculate % Away
        sma_signals = []
        smas = adv_tech.get("SMA", [])
        # We want to know if price is above or below specific SMAs
        target_smas = ["20", "50", "200"]
        
        for s in smas:
            ind_name = s.get("Indicator", "").replace("-SMA", "")
            val = get_float(s.get("Value"))
            
            if ind_name in target_smas and val > 0 and ltp > 0:
                diff = ((ltp - val) / val) * 100
                status = "Above" if diff > 0 else "Below"
                sma_signals.append(f"SMA {ind_name}: {status} ({round(diff, 1)}%)")

        # Parse EMA Signals - Calculate % Away
        ema_signals = []
        emas = adv_tech.get("EMA", [])
        target_emas = ["20", "50", "200"]
        
        for e in emas:
            ind_name = e.get("Indicator", "").replace("-EMA", "")
            val = get_float(e.get("Value"))
            
            if ind_name in target_emas and val > 0 and ltp > 0:
                diff = ((ltp - val) / val) * 100
                status = "Above" if diff > 0 else "Below"
                ema_signals.append(f"EMA {ind_name}: {status} ({round(diff, 1)}%)")
            
        # Parse Oscillators/Trend
        tech_inds = adv_tech.get("TechnicalIndicators", [])
        sentiment_summary = []
        for t in tech_inds:
            name = t.get("Indicator", "")
            action = t.get("Action", "")
            if "RSI" in name:
                sentiment_summary.append(f"RSI: {action}")
            elif "MACD" in name:
                 sentiment_summary.append(f"MACD: {action}")

        # Extract Pivots (Classic PP)
        pivots = adv_tech.get("Pivots", [])
        classic_pivot = "N/A"
        if pivots and isinstance(pivots, list):
             classic = pivots[0].get("Classic", {})
             classic_pivot = classic.get("PP", "N/A")

        stock_analysis.update({
            "Stock Price(₹)": ltp,
            "Index": ", ".join(indices_found) if indices_found else "N/A",
            "1 Day Returns(%)": get_float(tech.get("PPerchange", 0)),
            "1 Week Returns(%)": get_float(tech.get("PricePerchng1week", 0)),
            "1 Month Returns(%)": get_float(tech.get("PricePerchng1mon", 0)),
            "3 Month Returns(%)": get_float(tech.get("PricePerchng3mon", 0)),
            "1 Year Returns(%)": get_float(tech.get("PricePerchng1year", 0)),
            "RSI (14)": round(rsi_14, 2),
            "Gap Up %": 0.0,
            "SMA Status": " | ".join(sma_signals),
            "EMA Status": " | ".join(ema_signals),
            "Technical Sentiment": " | ".join(sentiment_summary),
            "Pivot Point": classic_pivot
        })
        
        analyzed_data.append(stock_analysis)

    # Save to JSON
    with open(output_file, "w") as f:
        json.dump(analyzed_data, f, indent=4)
        
    print(f"Successfully saved analysis for {len(analyzed_data)} stocks to {output_file}")

if __name__ == "__main__":
    analyze_all_stocks()
