"""
═══════════════════════════════════════════════════════════════
  EDL PIPELINE - MASTER RUNNER
  Run this single script to produce the complete:
  all_stocks_fundamental_analysis.json
═══════════════════════════════════════════════════════════════

Dependency Chain (Strict Order):

  PHASE 1: Core Data (Foundation)
    1. fetch_dhan_data.py          → dhan_data_response.json + master_isin_map.json
    2. fetch_fundamental_data.py   → fundamental_data.json

  PHASE 2: Parallel Enrichment (All depend on master_isin_map.json)
    3. fetch_company_filings.py    → company_filings/*.json  (Hybrid: LODR + Legacy)
    4. fetch_new_announcements.py  → all_company_announcements.json
    5. fetch_advanced_indicators.py→ advanced_indicator_data.json
    6. fetch_market_news.py        → market_news/*.json
    7. fetch_corporate_actions.py  → upcoming/history_corporate_actions.json
    8. fetch_surveillance_lists.py → nse_asm_list.json, nse_gsm_list.json
    9. fetch_circuit_stocks.py     → upper/lower_circuit_stocks.json
   10. fetch_bulk_block_deals.py   → bulk_block_deals.json
   11. fetch_incremental_price_bands.py → incremental_price_bands.json
   12. fetch_complete_price_bands.py    → complete_price_bands.json

  PHASE 3: Base Analysis (Builds the master JSON structure)
   13. bulk_market_analyzer.py     → all_stocks_fundamental_analysis.json (BASE)

  PHASE 4: Enrichment (Modifies master JSON in-place, ORDER MATTERS)
   14. advanced_metrics_processor.py → Adds ADR, RVOL, ATH, Turnover
   15. process_earnings_performance.py → Adds post-earnings returns
   16. add_corporate_events.py     → Adds Events, Announcements, News Feed (LAST!)

  PHASE 5: Optional (Standalone Data, not in master JSON)
    - fetch_all_ohlcv.py           → ohlcv_data/*.csv (SLOW, ~30 min)
    - fetch_fno_data.py            → fno_stocks_response.json
    - fetch_fno_lot_sizes.py       → fno_lot_sizes_cleaned.json
    - fetch_fno_expiry.py          → fno_expiry_calendar.json
    - fetch_all_indices.py         → all_indices_list.json
    - fetch_etf_data.py            → etf_data_response.json
"""

import subprocess
import sys
import os
import time
import shutil
import glob

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ═══════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════

# Set to True to also fetch OHLCV data (adds ~30 min)
FETCH_OHLCV = False

# Set to True to also fetch FNO, ETF, Indices (standalone data)
FETCH_OPTIONAL = False

# Auto-delete intermediate files after pipeline succeeds
# Only keeps: all_stocks_fundamental_analysis.json + ohlcv_data/
CLEANUP_INTERMEDIATE = True

# ═══════════════════════════════════════════════════

# Intermediate files that are ONLY used between pipeline stages
# These are ephemeral: fetched from APIs → consumed → deleted
INTERMEDIATE_FILES = [
    "master_isin_map.json",
    "dhan_data_response.json",
    "fundamental_data.json",
    "advanced_indicator_data.json",
    "all_company_announcements.json",
    "upcoming_corporate_actions.json",
    "history_corporate_actions.json",
    "nse_asm_list.json",
    "nse_gsm_list.json",
    "bulk_block_deals.json",
    "upper_circuit_stocks.json",
    "lower_circuit_stocks.json",
    "incremental_price_bands.json",
    "complete_price_bands.json",
    "nse_equity_list.csv",
]

INTERMEDIATE_DIRS = [
    "company_filings",
    "market_news",
]


def run_script(script_name, phase_label):
    """Run a Python script and return success/failure."""
    script_path = os.path.join(BASE_DIR, script_name)
    
    if not os.path.exists(script_path):
        print(f"  ⚠️  SKIP: {script_name} not found.")
        return False
    
    print(f"  ▶ Running {script_name}...")
    start = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            timeout=600  # 10 min max per script
        )
        elapsed = time.time() - start
        
        if result.returncode == 0:
            # Extract last meaningful line from stdout
            lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip()]
            summary = lines[-1] if lines else "Done"
            print(f"  ✅ {script_name} ({elapsed:.1f}s) → {summary}")
            return True
        else:
            err_lines = [l.strip() for l in result.stderr.strip().split("\n") if l.strip()]
            err_msg = err_lines[-1] if err_lines else "Unknown error"
            print(f"  ❌ {script_name} FAILED ({elapsed:.1f}s) → {err_msg}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"  ⏰ {script_name} TIMED OUT (>600s)")
        return False
    except Exception as e:
        print(f"  ❌ {script_name} EXCEPTION: {e}")
        return False


def cleanup_intermediate():
    """Delete all intermediate files and directories, keeping only the final output."""
    removed_files = 0
    removed_dirs = 0
    freed_bytes = 0
    
    for f in INTERMEDIATE_FILES:
        fp = os.path.join(BASE_DIR, f)
        if os.path.exists(fp):
            freed_bytes += os.path.getsize(fp)
            os.remove(fp)
            removed_files += 1
    
    for d in INTERMEDIATE_DIRS:
        dp = os.path.join(BASE_DIR, d)
        if os.path.exists(dp):
            # Count size before removing
            for root, dirs, files in os.walk(dp):
                for file in files:
                    freed_bytes += os.path.getsize(os.path.join(root, file))
            shutil.rmtree(dp)
            removed_dirs += 1
    
    freed_mb = freed_bytes / (1024 * 1024)
    print(f"  🗑️  Cleaned: {removed_files} files + {removed_dirs} dirs ({freed_mb:.1f} MB freed)")


def main():
    overall_start = time.time()
    
    print("═" * 60)
    print("  EDL PIPELINE - FULL DATA REFRESH")
    print("═" * 60)
    
    results = {}
    
    # ─── PHASE 1: Core Data ───
    print("\n📦 PHASE 1: Core Data (Foundation)")
    print("─" * 40)
    results["fetch_dhan_data.py"] = run_script("fetch_dhan_data.py", "Phase 1")
    
    if not results["fetch_dhan_data.py"]:
        print("\n🛑 CRITICAL: fetch_dhan_data.py failed. Cannot continue.")
        print("   This script produces master_isin_map.json which ALL other scripts need.")
        return
    
    results["fetch_fundamental_data.py"] = run_script("fetch_fundamental_data.py", "Phase 1")
    
    # Download NSE listing dates CSV
    print("  ▶ Downloading NSE Listing Dates...")
    csv_path = os.path.join(BASE_DIR, "nse_equity_list.csv")
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", csv_path,
             "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
             "--http1.1",
             "--header", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"],
            capture_output=True, text=True, timeout=30
        )
        print(f"  ✅ NSE Listing Dates downloaded.")
    except:
        print(f"  ⚠️  NSE CSV download failed (non-critical).")

    # ─── PHASE 2: Parallel Enrichment ───
    print("\n📡 PHASE 2: Data Enrichment (Fetching)")
    print("─" * 40)
    
    phase2_scripts = [
        "fetch_company_filings.py",
        "fetch_new_announcements.py",
        "fetch_advanced_indicators.py",
        "fetch_market_news.py",
        "fetch_corporate_actions.py",
        "fetch_surveillance_lists.py",
        "fetch_circuit_stocks.py",
        "fetch_bulk_block_deals.py",
        "fetch_incremental_price_bands.py",
        "fetch_complete_price_bands.py",
    ]
    
    for script in phase2_scripts:
        results[script] = run_script(script, "Phase 2")
    
    # ─── PHASE 2.5: OHLCV (Optional, Slow) ───
    if FETCH_OHLCV:
        print("\n📊 PHASE 2.5: OHLCV History (Optional - SLOW)")
        print("─" * 40)
        results["fetch_all_ohlcv.py"] = run_script("fetch_all_ohlcv.py", "Phase 2.5")
    
    # ─── PHASE 3: Base Analysis ───
    print("\n🔬 PHASE 3: Base Analysis (Building Master JSON)")
    print("─" * 40)
    results["bulk_market_analyzer.py"] = run_script("bulk_market_analyzer.py", "Phase 3")
    
    if not results["bulk_market_analyzer.py"]:
        print("\n🛑 CRITICAL: bulk_market_analyzer.py failed.")
        print("   Cannot produce all_stocks_fundamental_analysis.json.")
        return
    
    # ─── PHASE 4: Enrichment (Order Matters!) ───
    print("\n✨ PHASE 4: Enrichment (Injecting into Master JSON)")
    print("─" * 40)
    
    # 4a. Advanced Metrics (ADR, RVOL, ATH) - needs ohlcv_data/
    results["advanced_metrics_processor.py"] = run_script("advanced_metrics_processor.py", "Phase 4")
    
    # 4b. Earnings Performance - needs company_filings/ + ohlcv_data/
    results["process_earnings_performance.py"] = run_script("process_earnings_performance.py", "Phase 4")
    
    # 4c. Corporate Events + News Feed (MUST BE LAST)
    results["add_corporate_events.py"] = run_script("add_corporate_events.py", "Phase 4")
    
    # ─── PHASE 5: Optional Standalone Data ───
    if FETCH_OPTIONAL:
        print("\n📋 PHASE 5: Optional Standalone Data")
        print("─" * 40)
        for script in ["fetch_fno_data.py", "fetch_fno_lot_sizes.py", 
                       "fetch_fno_expiry.py", "fetch_all_indices.py", "fetch_etf_data.py"]:
            results[script] = run_script(script, "Phase 5")
    
    # ─── CLEANUP: Remove intermediate files ───
    if CLEANUP_INTERMEDIATE:
        print("\n🧹 CLEANUP: Removing intermediate files...")
        print("─" * 40)
        cleanup_intermediate()
    
    # ─── FINAL REPORT ───
    total_time = time.time() - overall_start
    success = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    
    print("\n" + "═" * 60)
    print("  PIPELINE COMPLETE")
    print("═" * 60)
    print(f"  Total Time:  {total_time:.1f}s ({total_time/60:.1f} min)")
    print(f"  Successful:  {success}/{len(results)}")
    print(f"  Failed:      {failed}/{len(results)}")
    
    if failed > 0:
        print("\n  Failed Scripts:")
        for script, ok in results.items():
            if not ok:
                print(f"    ❌ {script}")
    
    master = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json")
    if os.path.exists(master):
        size_mb = os.path.getsize(master) / (1024 * 1024)
        print(f"\n  📄 Output: all_stocks_fundamental_analysis.json ({size_mb:.1f} MB)")
    
    if CLEANUP_INTERMEDIATE:
        print(f"  🧹 Only final output remains. All intermediate data purged.")
    
    print("═" * 60)


if __name__ == "__main__":
    main()
