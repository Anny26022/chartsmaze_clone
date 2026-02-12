import json
import csv

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
    input_file = "fundamental_data.json"
    output_file = "all_stocks_fundamental_analysis.json"
    
    print("Loading fundamental data...")
    try:
        with open(input_file, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return

    # Load Listing Dates from NSE CSV
    listing_date_map = {}
    try:
        with open("nse_equity_list.csv", "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Map Symbol -> Date of Listing
                # CSV Headers: SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING,...
                sym = row.get("SYMBOL")
                date_list = row.get(" DATE OF LISTING") # Note: CSV might have leading space
                if not date_list:
                     date_list = row.get("DATE OF LISTING") # Try without space
                
                if sym and date_list:
                    listing_date_map[sym] = date_list
        print(f"Loaded listing dates for {len(listing_date_map)} symbols.")
    except FileNotFoundError:
        print("Warning: nse_equity_list.csv not found. Listing dates will be N/A.")

    # Load Technical Data from Dhan Response
    dhan_tech_map = {}
    dhan_file = "dhan_data_response.json"
    try:
        with open(dhan_file, "r") as f:
            dhan_data = json.load(f)
            # Create a lookup map: Symbol -> {Tech Data}
            for item in dhan_data:
                sym = item.get("Sym")
                if sym:
                    dhan_tech_map[sym] = item
        print(f"Loaded technical data for {len(dhan_tech_map)} symbols.")
    except FileNotFoundError:
        print(f"Warning: {dhan_file} not found. Technicals will be 0.")

    analyzed_data = []
    total_stocks = len(data)
    print(f"Analyzing {total_stocks} stocks...")

    for item in data:
        symbol = item.get("Symbol", "UNKNOWN")
        
        # --- Extract Data Sections ---
        cq = item.get("incomeStat_cq", {})
        cy = item.get("incomeStat_cy", {})
        ttm_cy = item.get("TTM_cy", {})
        cv = item.get("CV", {})
        roce_roe = item.get("roce_roe", {})
        shp = item.get("sHp", {})
        bs_c = item.get("bs_c", {})

        # --- 1. Profit & Loss (Net Profit) ---
        np_latest = get_value_from_pipe_string(cq.get("NET_PROFIT"), 0)
        np_prev = get_value_from_pipe_string(cq.get("NET_PROFIT"), 1)
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
        eps_last_year_annual = get_value_from_pipe_string(cy.get("EPS"), 0) # Index 0 per User preference
        eps_2_years_back_annual = get_value_from_pipe_string(cy.get("EPS"), 1)

        qoq_eps = calculate_change(eps_latest, eps_prev)
        yoy_eps = calculate_change(eps_latest, eps_last_year_q)

        # --- 3. Sales (Net Sales) ---
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

        # --- 5. Ratios & Valuation ---
        roe = get_float(roce_roe.get("ROE"))
        roce = get_float(roce_roe.get("ROCE"))
        pe = get_float(cv.get("STOCK_PE"))
        
        non_current_liab = get_value_from_pipe_string(bs_c.get("NON_CURRENT_LIABILITIES"), 0)
        total_equity = get_value_from_pipe_string(bs_c.get("TOTAL_EQUITY"), 0)
        de_ratio = non_current_liab / total_equity if total_equity != 0 else 0.0

        peg = 0.0
        if yoy_eps > 0 and pe > 0: # Ensure positive divisors
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
        
        # Free Float Calculation (100 - Promoter Holding)
        # Note: If no shareholding data, promoter_latest will be 0.
        # We check if 'sHp' key exists to avoid false 100% float on missing data.
        free_float_pct = 0.0
        float_shares_cr = 0.0
        
        if shp and promoter_latest >= 0:
             free_float_pct = 100.0 - promoter_latest
        
        # Float Shares Calculation: (Mcap / Price) * Free Float %
        # Mcap is in Cr (usually) in Dhan response, but here we don't have Mcap in 'fundamental_data.json'.
        # Wait, 'CV' has 'MARKET_CAP'. Let's verify unit. 
        # CV -> MARKET_CAP: "660.67". This is typically in Crores for Indian markets.
        mcap_cr = get_float(cv.get("MARKET_CAP"))
        
        if mcap_cr > 0 and free_float_pct > 0:
             # Total Float Value (Cr) = Mcap * (Free Float / 100)
             # But user asked for Float SHARES (Cr).
             # We need Price to get Shares. CV has 'PRICE_TO_BOOK_VALUE' & 'STOCK_PE' but not Price directly.
             # However, Mcap = Price * Shares. So Shares = Mcap / Price.
             # Float Shares = (Mcap / Price) * (Free Float / 100).
             # Missing STOCK PRICE in fundamental_data.json 'CV'.
             # We can try to infer Price from PE * EPS (Latest Annual).
             # Price ~ PE * TTM_EPS.
             ttm_eps = get_float(ttm_cy.get("EPS"))
             
             if pe > 0 and ttm_eps > 0:
                 inferred_price = pe * ttm_eps
                 if inferred_price > 0:
                     total_shares_cr = mcap_cr / inferred_price
                     float_shares_cr = total_shares_cr * (free_float_pct / 100.0)
             
             # Fallback: If we can't infer price, we return Float Market Cap?
             # User asked for "Float Shares (Cr.)".
             # Better approach: Just output Free Float % which is reliable.
             # Calculated Float Shares might be noisy if Price inference is off.
             # Let's try to calculate it anyway if we have the data.


        # --- 7. Meta ---
        latest_quarter_str = cq.get("YEAR", "").split('|')[0] if cq.get("YEAR") else "N/A"
        listing_date = listing_date_map.get(symbol, "N/A")

        stock_analysis = {
            "Symbol": symbol,
            "Name": item.get("Name", ""),
            "Listing Date": listing_date,
            "Latest Quarter": latest_quarter_str,
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
            "Forward P/E": round(forward_pe, 2),
            "Historical P/E 5": 0.0
        }

        # --- 8. Technicals ---
        tech = dhan_tech_map.get(symbol, {})
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

        # Day Range
        day_high = get_float(tech.get("Min15HighCurrentCandle", 0)) # Using nearby timeframe high as proxy if daily OHLC missing
        # Dhan data doesn't guarantee 'DayHigh', but has 'High1Wk'. Better use 'PPerchange'.
        
        stock_analysis.update({
            "Stock Price(₹)": ltp,
            "1 Day Returns(%)": get_float(tech.get("PPerchange", 0)),
            "1 Week Returns(%)": get_float(tech.get("PricePerchng1week", 0)),
            "1 Month Returns(%)": get_float(tech.get("PricePerchng1mon", 0)),
            "3 Month Returns(%)": get_float(tech.get("PricePerchng3mon", 0)),
            "1 Year Returns(%)": get_float(tech.get("PricePerchng1year", 0)),
            "Day SMA 200": sma_200,
            "% Away from SMA 200": round(pct_from_sma_200, 2),
            "Day SMA 50": sma_50,
            "% Away from SMA 50": round(pct_from_sma_50, 2),
            "RSI (14)": round(rsi_14, 2),
            "Gap Up %": 0.0
        })
        
        analyzed_data.append(stock_analysis)

    # Save to JSON
    with open(output_file, "w") as f:
        json.dump(analyzed_data, f, indent=4)
        
    print(f"Successfully saved analysis for {len(analyzed_data)} stocks to {output_file}")

if __name__ == "__main__":
    analyze_all_stocks()
