import json
import sys

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

def analyze_stock(symbol_to_find):
    input_file = "fundamental_data.json"
    
    try:
        with open(input_file, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return

    stock_data = None
    for item in data:
        if item.get("Symbol") == symbol_to_find:
            stock_data = item
            break
    
    if not stock_data:
        print(f"Stock '{symbol_to_find}' not found in database.")
        return

    # --- Extract Data Sections ---
    cq = stock_data.get("incomeStat_cq", {})
    cy = stock_data.get("incomeStat_cy", {})
    ttm_cy = stock_data.get("TTM_cy", {})
    cv = stock_data.get("CV", {})
    roce_roe = stock_data.get("roce_roe", {})
    shp = stock_data.get("sHp", {})
    bs_c = stock_data.get("bs_c", {}) # Balance Sheet Consolidated

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
        # CAGR formula: (End/Start)^(1/n) - 1
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
    
    # Debt to Equity
    non_current_liab = get_value_from_pipe_string(bs_c.get("NON_CURRENT_LIABILITIES"), 0)
    total_equity = get_value_from_pipe_string(bs_c.get("TOTAL_EQUITY"), 0)
    de_ratio = non_current_liab / total_equity if total_equity != 0 else 0.0

    # PEG Ratio (PE / Earnings Growth)
    # Using YoY Quarterly EPS growth as proxy for earnings growth momentum
    peg = 0.0
    if yoy_eps > 0:
        peg = pe / yoy_eps

    # Forward P/E (Estimate: Current Price / (Latest EPS * 4))
    forward_pe = 0.0
    if eps_latest > 0:
        annualized_eps = eps_latest * 4
        # We need price. We can back-calculate from PE if EPS is known, but better to use PE directly
        # If PE = Price / TTM_EPS, then Price = PE * TTM_EPS
        # Forward PE = (PE * TTM_EPS) / Forward_EPS
        # Simplifying: Forward PE = PE * (TTM_EPS / Annualized_Latest_EPS)
        ttm_eps = get_float(ttm_cy.get("EPS"))
        if annualized_eps > 0:
             forward_pe = pe * (ttm_eps / annualized_eps)

    # Historical P/E 5 (Approximation not directly available, returning 0/Placeholder)
    historical_pe_5 = 0.0 

    # --- 6. Shareholding ---
    fii_latest = get_value_from_pipe_string(shp.get("FII"), 0)
    fii_prev = get_value_from_pipe_string(shp.get("FII"), 1)
    fii_change_qoq = fii_latest - fii_prev # Percentage point change

    dii_latest = get_value_from_pipe_string(shp.get("DII"), 0)
    dii_prev = get_value_from_pipe_string(shp.get("DII"), 1)
    dii_change_qoq = dii_latest - dii_prev # Percentage point change

    # --- 7. Meta ---
    latest_quarter_str = cq.get("YEAR", "").split('|')[0] if cq.get("YEAR") else "N/A"


    # --- Print Output in Specified Format ---
    print(f"--- Analysis for {symbol_to_find} ---")
    
    print("\nNet Profit Latest Quarter:", np_latest)
    print("Net Profit Previous Quarter:", np_prev)
    print("Net Profit 2 Quarters Back:", get_value_from_pipe_string(cq.get("NET_PROFIT"), 2))
    print("Net Profit 3 Quarters Back:", np_3_back)
    print("Net Profit Last Year Quarter:", np_last_year_q)
    print(f"QoQ % Net Profit Latest: {qoq_np:.2f}%")
    print(f"YoY % Net Profit Latest: {yoy_np:.2f}%")
    
    print("\nEPS Latest Quarter:", eps_latest)
    print("EPS Previous Quarter:", eps_prev)
    print("EPS 2 Quarters Back:", eps_2_back)
    print("EPS 3 Quarters Back:", eps_3_back)
    print("EPS Last Year Quarter:", eps_last_year_q)
    print(f"QoQ % EPS Latest: {qoq_eps:.2f}%")
    print(f"YoY % EPS Latest: {yoy_eps:.2f}%")
    print("EPS Last Year:", eps_last_year_annual)
    print("EPS 2 Years Back:", eps_2_years_back_annual)
    
    print("\nSales Latest Quarter:", sales_latest)
    print("Sales Previous Quarter:", sales_prev)
    print("Sales 2 Quarters Back:", sales_2_back)
    print("Sales 3 Quarters Back:", sales_3_back)
    print("Sales Last Year Quarter:", sales_last_year_q)
    print(f"QoQ % Sales Latest: {qoq_sales:.2f}%")
    print(f"YoY % Sales Latest: {yoy_sales:.2f}%")
    print(f"Sales Growth 5 Years(%): {sales_growth_5y:.2f}%")
    
    print("\nOPM Latest Quarter:", opm_latest)
    print("OPM Previous Quarter:", opm_prev)
    print("OPM 2 Quarters Back:", opm_2_back)
    print("OPM 3 Quarters Back:", opm_3_back)
    print("OPM Last Year Quarter:", opm_last_year_q)
    print(f"QoQ % OPM Latest: {qoq_opm:.2f}%")
    print(f"YoY % OPM Latest: {yoy_opm:.2f}%")
    
    print("\nLatest Quarter:", latest_quarter_str)
    print(f"ROE(%): {roe}")
    print(f"ROCE(%): {roce}")
    print(f"D/E: {de_ratio:.2f}")
    print(f"OPM TTM(%): {opm_ttm}")
    print(f"P/E: {pe}")
    print(f"FII % change QoQ: {fii_change_qoq:.2f}%")
    print(f"DII % change QoQ: {dii_change_qoq:.2f}%")
    print(f"PEG: {peg:.2f}")
    print(f"Forward P/E: {forward_pe:.2f}")
    print(f"Historical P/E 5: {historical_pe_5} (Data Unavailable)")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        analyze_stock(sys.argv[1])
    else:
        print("Usage: python3 analyze_stock.py <SYMBOL>")
        print("Example: python3 analyze_stock.py RELIANCE")
