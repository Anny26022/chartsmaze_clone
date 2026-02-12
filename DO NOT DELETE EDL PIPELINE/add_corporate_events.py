import json
import os
import glob
from datetime import datetime, timedelta

def map_refined_events():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    master_file = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
    upcoming_file = os.path.join(BASE_DIR, "upcoming_corporate_actions.json")
    filings_dir = os.path.join(BASE_DIR, "company_filings")
    asm_file = os.path.join(BASE_DIR, "nse_asm_list.json")
    gsm_file = os.path.join(BASE_DIR, "nse_gsm_list.json")
    deals_file = os.path.join(BASE_DIR, "bulk_block_deals.json")
    circuit_revision_file = os.path.join(BASE_DIR, "incremental_price_bands.json")
    
    if not os.path.exists(master_file):
        print(f"Error: {master_file} not found.")
        return
        
    print("Loading master data...")
    with open(master_file, "r") as f:
        master_data = json.load(f)
        
    refined_map = {} # Maps symbol to a list of strings/icons

    def add_event(sym, event_str):
        if sym not in refined_map:
            refined_map[sym] = []
        if event_str not in refined_map[sym]:
            refined_map[sym].append(event_str)

    # --- 1. Surveillance Mapping ---
    print("Processing Surveillance (★: LTASM, ★: STASM)...")
    if os.path.exists(asm_file):
        with open(asm_file, "r") as f:
            asm_data = json.load(f)
            for item in asm_data:
                sym = item.get("Symbol")
                stage = item.get("Stage", "")
                if sym:
                    if "LTASM" in stage:
                        add_event(sym, "★: LTASM")
                    elif "STASM" in stage:
                        add_event(sym, "★: STASM")

    # --- 2. Corporate Actions (Bonus, Splts, Dividends, Results) ---
    print("Processing Corporate Actions (⏰, 💸, ✂️, 🎁, 📈)...")
    if os.path.exists(upcoming_file):
        with open(upcoming_file, "r") as f:
            upcoming_data = json.load(f)
            today = datetime.now()
            # Standard window for actions is typically 30 days
            action_limit = today + timedelta(days=30)
            # Short window for results is 14 days
            results_limit = today + timedelta(days=14)
            
            for event in upcoming_data:
                sym = event.get("Symbol")
                etype = event.get("Type", "")
                edate_str = event.get("ExDate")
                if not sym or not edate_str: continue
                
                try:
                    edate = datetime.strptime(edate_str, "%Y-%m-%d")
                    # Only map if it's today or in the future within the window
                    if today.date() <= edate.date() <= action_limit.date():
                        d_str = edate.strftime("%d-%b")
                        if "QUARTERLY" in etype and edate.date() <= results_limit.date():
                            add_event(sym, f"⏰: Results ({d_str})")
                        elif "DIVIDEND" in etype:
                            add_event(sym, f"💸: Dividend ({d_str})")
                        elif "BONUS" in etype:
                            add_event(sym, f"🎁: Bonus ({d_str})")
                        elif "SPLIT" in etype:
                            add_event(sym, f"✂️: Split ({d_str})")
                        elif "RIGHTS" in etype:
                            add_event(sym, f"📈: Rights ({d_str})")
                except:
                    pass

    # --- 3. Circuit Limit Revisions ---
    print("Processing Circuit Revisions (#: -ve/ +ve Circuit Limit Revision)...")
    if os.path.exists(circuit_revision_file):
        with open(circuit_revision_file, "r") as f:
            rev_data = json.load(f)
            for item in rev_data:
                sym = item.get("Symbol")
                f_band = item.get("From")
                t_band = item.get("To")
                if sym and f_band and t_band:
                    try:
                        if float(t_band) < float(f_band):
                            add_event(sym, "#: -ve Circuit Limit Revision")
                        elif float(t_band) > float(f_band):
                            add_event(sym, "#: +ve Circuit Limit Revision")
                    except:
                        pass

    # --- 4. Deals ---
    print("Processing Deals (📦: Block Deal)...")
    if os.path.exists(deals_file):
        with open(deals_file, "r") as f:
            deals_data = json.load(f)
            # Find deals from last 7 days
            today = datetime.now()
            recent_limit = today - timedelta(days=7)
            for deal in deals_data:
                sym = deal.get("sym")
                dtype = deal.get("deal", "")
                d_date_str = deal.get("date", "").split(" ")[0]
                if sym and d_date_str:
                    try:
                        d_date = datetime.strptime(d_date_str, "%Y-%m-%d")
                        if d_date >= recent_limit:
                            if "BLOCK" in dtype or "BULK" in dtype:
                                add_event(sym, "📦: Block Deal")
                    except:
                        pass

    # --- 5. Insider Trading & Recent Announcements Mapping ---
    print("Processing Insider Trades & Recent Headlines...")
    news_map = {} # Maps sym -> list of dicts
    if os.path.exists(filings_dir):
        filing_files = glob.glob(os.path.join(filings_dir, "*_filings.json"))
        today = datetime.now()
        recent_limit = today - timedelta(days=15)
        for ff in filing_files:
            sym = os.path.basename(ff).replace("_filings.json", "")
            try:
                with open(ff, "r") as f:
                    f_data = json.load(f)
                    items = f_data.get("data", [])
                    if not items: continue

                    # 1. Capture top 5 structured filings
                    news_map[sym] = []
                    for item in items[:5]:
                        headline = (item.get("caption") or item.get("descriptor") or item.get("news_body") or "N/A")
                        headline = " ".join(headline.split())
                        news_map[sym].append({
                            "Date": item.get("news_date", "N/A"),
                            "Headline": headline,
                            "URL": item.get("file_url") or "N/A"
                        })

                    # 2. Check for Insider Trading in recent items
                    for item in items:
                        desc = (item.get("descriptor") or "").lower()
                        caption = (item.get("caption") or "").lower()
                        cat = (item.get("cat") or "").lower()
                        body = (item.get("news_body") or "").lower()
                        n_date_str = item.get("news_date", "").split(" ")[0]
                        
                        if n_date_str:
                            n_date = datetime.strptime(n_date_str, "%Y-%m-%d")
                            if n_date >= recent_limit:
                                is_insider = False
                                full_text = f"{desc} {caption} {cat} {body}"
                                trade_keywords = ["regulation 7(2)", "reg 7(2)", "inter-se transfer", "form c", "continual disclosure"]
                                if any(k in full_text for k in trade_keywords):
                                    is_insider = True
                                elif ("insider trading" in full_text or "sebi (pit)" in full_text or "sebi pit" in full_text):
                                    if "trading window" not in full_text and "closure" not in full_text:
                                        is_insider = True
                                if is_insider:
                                    add_event(sym, "🔑: Insider Trading")
                                    break
            except:
                continue

    # --- 6. Recent Results & Live Announcements (New API) ---
    print("Processing Recent Results & Live Headlines (📊)...")
    ann_file = os.path.join(BASE_DIR, "all_company_announcements.json")
    if os.path.exists(ann_file):
        with open(ann_file, "r") as f:
            ann_data = json.load(f)
            today = datetime.now()
            marker_limit = today - timedelta(days=7)
            for ann in ann_data:
                sym = ann.get("Symbol")
                event_text = (ann.get("Event") or "")
                etype = ann.get("Type", "")
                date_str = ann.get("Date", "")
                
                if sym and date_str:
                    try:
                        # A. Add Markers (Icon)
                        a_date = datetime.strptime(date_str.split(" ")[0], "%Y-%m-%d")
                        if a_date >= marker_limit:
                            if "results are out" in event_text.lower() or etype == "Results Update":
                                add_event(sym, "📊: Results Recently Out")

                        # B. Update News List if not already present
                        if sym not in news_map:
                            news_map[sym] = []
                        
                        # Check if this exact headline exists in the existing list
                        exists = False
                        for existing in news_map[sym]:
                            if event_text.lower() in existing["Headline"].lower():
                                exists = True
                                break
                        
                        if not exists:
                            news_map[sym].insert(0, {
                                "Date": date_str,
                                "Headline": event_text,
                                "URL": "N/A"
                            })
                            # Keep only top 5
                            news_map[sym] = news_map[sym][:5]
                    except:
                        pass

    # --- 7. Market News Feed (Sentiment & General News) ---
    print("Processing Market News Feed (Sentiment Analysis)...")
    market_news_dir = os.path.join(BASE_DIR, "market_news")
    news_feed_map = {}
    
    if os.path.exists(market_news_dir):
        news_files = glob.glob(os.path.join(market_news_dir, "*_news.json"))
        for nf in news_files:
            try:
                with open(nf, "r") as f:
                    n_data = json.load(f)
                    sym = n_data.get("Symbol")
                    news_list = n_data.get("News", [])
                    
                    if sym and news_list:
                        # Take top 5 items
                        formatted_news = []
                        for item in news_list[:5]:
                            formatted_news.append({
                                "Title": item.get("Title"),
                                "Sentiment": item.get("Sentiment"),
                                "Date": item.get("PublishDate") # Raw timestamp
                            })
                        news_feed_map[sym] = formatted_news
            except:
                continue

    # --- Update Master JSON ---
    print(f"Applying markers, headlines, and news to {len(master_data)} stocks...")
    for stock in master_data:
        sym = stock.get("Symbol")
        
        # 1. Update Events
        events = refined_map.get(sym, [])
        stock["Event Markers"] = " | ".join(events) if events else "N/A"

        # 2. Update Recent Announcements (Top 5 - Regulatory)
        stock["Recent Announcements"] = news_map.get(sym, [])[:5]
        
        # 3. Update News Feed (Top 5 - Market/Media)
        stock["News Feed"] = news_feed_map.get(sym, [])

    with open(master_file, "w") as f:
        json.dump(master_data, f, indent=4, ensure_ascii=False)
        
    print(f"Successfully updated master JSON with comprehensive markers.")

if __name__ == "__main__":
    map_refined_events()
