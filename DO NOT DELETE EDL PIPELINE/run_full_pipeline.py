"""
═══════════════════════════════════════════════════════════════
  EDL PIPELINE - MASTER RUNNER
  Run this single script to produce the complete:
  all_stocks_fundamental_analysis.json.gz
═══════════════════════════════════════════════════════════════

Dependency Chain (Strict Order):

  PHASE 1: Core Data (Foundation)
    1. fetch_dhan_data.py          → dhan_data_response.json + master_isin_map.json
    2. fetch_fundamental_data.py   → fundamental_data.json

  PHASE 2: Data Enrichment (All depend on master_isin_map.json)
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

  PHASE 2.5: OHLCV Data (Smart incremental — auto-enabled)
   13. fetch_all_ohlcv.py          → ohlcv_data/*.csv

  PHASE 3: Base Analysis (Builds the master JSON structure)
   14. bulk_market_analyzer.py     → all_stocks_fundamental_analysis.json (BASE)

  PHASE 4: Enrichment (Modifies master JSON in-place, ORDER MATTERS)
   15. advanced_metrics_processor.py → Adds ADR, RVOL, ATH, Turnover
   16. process_earnings_performance.py → Adds post-earnings returns
   17. enrich_fno_data.py          → Adds F&O flag, Lot Size, Next Expiry
   18. add_corporate_events.py     → Adds Events, Announcements, News Feed (LAST!)

  PHASE 5: Compression
       → all_stocks_fundamental_analysis.json.gz

  PHASE 6: Optional (Standalone Data, not in master JSON)
     - fetch_etf_data.py            → etf_data_response.json
"""

import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass

from pipeline_utils import BASE_DIR, compress_file


# ═══════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════

def env_bool(name, default):
    """Read a boolean env var while preserving the supplied default."""
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False

    print(f"  ⚠️  Ignoring invalid {name}={value!r}; using {default}.")
    return default


# OHLCV: Auto-detect mode.
# Override with EDL_FETCH_OHLCV=0/1.
FETCH_OHLCV = env_bool("EDL_FETCH_OHLCV", True)

# Optional standalone ETF scan. Override with EDL_FETCH_OPTIONAL=0/1.
FETCH_OPTIONAL = env_bool("EDL_FETCH_OPTIONAL", False)

# Auto-delete intermediate files after pipeline succeeds.
# Override with EDL_CLEANUP_INTERMEDIATE=0/1.
CLEANUP_INTERMEDIATE = env_bool("EDL_CLEANUP_INTERMEDIATE", True)

# ═══════════════════════════════════════════════════

# Intermediate files that are ONLY used between pipeline stages
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
    "all_stocks_fundamental_analysis.json",  # Raw JSON cleaned up after .gz is made
]

INTERMEDIATE_DIRS = [
    "company_filings",
    "market_news",
]

FILES_TO_COMPRESS = {
    "all_stocks_fundamental_analysis.json": "all_stocks_fundamental_analysis.json.gz",
    "sector_analytics.json": "sector_analytics.json.gz",
    "market_breadth.csv": "market_breadth.json.gz",
}

PHASE2_SCRIPTS = [
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
    "fetch_all_indices.py",
]

PHASE4_SCRIPTS = [
    "advanced_metrics_processor.py",
    "process_earnings_performance.py",
    "enrich_fno_data.py",
    "process_market_breadth.py",
    "process_historical_market_breadth.py",
    "add_corporate_events.py",
]


@dataclass
class ScriptResult:
    ok: bool
    required: bool
    elapsed: float = 0.0
    returncode: int = 0
    error: str = ""


def run_script(script_name, phase_label, required=False):
    """Run a Python script and report whether it completed successfully."""
    script_path = os.path.join(BASE_DIR, script_name)
    
    if not os.path.exists(script_path):
        print(f"  ⚠️  SKIP: {script_name} not found.")
        return ScriptResult(False, required, error="missing")
    
    print(f"  ▶ Running {script_name}...")
    start = time.time()
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=BASE_DIR,
            text=True,
            timeout=1800
        )
        elapsed = time.time() - start
        
        if result.returncode == 0:
            print(f"  ✅ {script_name} ({elapsed:.1f}s)")
            return ScriptResult(True, required, elapsed=elapsed)

        print(f"  ❌ {script_name} FAILED ({elapsed:.1f}s, exit {result.returncode})")
        return ScriptResult(False, required, elapsed=elapsed, returncode=result.returncode)
            
    except subprocess.TimeoutExpired:
        print(f"  ⏰ {script_name} TIMED OUT (>30 min)")
        return ScriptResult(False, required, elapsed=1800, error="timeout")
    except Exception as e:
        print(f"  ❌ {script_name} EXCEPTION: {e}")
        return ScriptResult(False, required, error=str(e))


def compress_output():
    """Compress final JSONs to .json.gz for ultra compression."""
    total_raw = 0
    total_gz = 0
    
    for filename, output_name in FILES_TO_COMPRESS.items():
        raw_size, gz_size = compress_file(filename, output_name)
        if raw_size:
            total_raw += raw_size
            total_gz += gz_size
        else:
            print(f"  ⚠️  {filename} not found to compress.")
            
    ratio = (1 - total_gz / total_raw) * 100 if total_raw > 0 else 0
    print(f"  📦 Compressed: {total_raw / (1024*1024):.1f} MB → {total_gz / (1024*1024):.1f} MB ({ratio:.0f}% reduction)")
    return total_raw, total_gz


def download_nse_listing_dates():
    """Download NSE listing dates used by the base analyzer."""
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
        if result.returncode == 0 and os.path.getsize(csv_path) > 0:
            print("  ✅ NSE Listing Dates downloaded.")
            return True
        print(f"  ⚠️  NSE CSV download failed (exit {result.returncode}, non-critical).")
    except Exception as e:
        print(f"  ⚠️  NSE CSV download failed: {e} (non-critical).")
    return False


def cleanup_intermediate():
    """Delete all intermediate files and directories, keeping only .json.gz + ohlcv_data/."""
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
    results["fetch_dhan_data.py"] = run_script("fetch_dhan_data.py", "Phase 1", required=True)
    
    if not results["fetch_dhan_data.py"].ok:
        print("\n🛑 CRITICAL: fetch_dhan_data.py failed. Cannot continue.")
        print("   This script produces master_isin_map.json which ALL other scripts need.")
        return
    
    results["fetch_fundamental_data.py"] = run_script("fetch_fundamental_data.py", "Phase 1", required=True)

    if not results["fetch_fundamental_data.py"].ok:
        print("\n🛑 CRITICAL: fetch_fundamental_data.py failed. Cannot continue.")
        print("   This script produces fundamental_data.json for the base analyzer.")
        return
    
    download_nse_listing_dates()

    # ─── PHASE 2: Data Enrichment ───
    print("\n📡 PHASE 2: Data Enrichment (Fetching)")
    print("─" * 40)
    
    for script in PHASE2_SCRIPTS:
        results[script] = run_script(script, "Phase 2")
    
    # ─── PHASE 2.5: OHLCV (Smart Incremental) ───
    if FETCH_OHLCV:
        print("\n📊 PHASE 2.5: OHLCV History (Smart Incremental)")
        print("─" * 40)
        
        # 1. Stocks
        results["fetch_all_ohlcv.py"] = run_script("fetch_all_ohlcv.py", "Phase 2.5")
        
        # 2. Indices (New Specialized High-Speed)
        results["fetch_indices_ohlcv.py"] = run_script("fetch_indices_ohlcv.py", "Phase 2.5")
    
    # ─── PHASE 3: Base Analysis ───
    print("\n🔬 PHASE 3: Base Analysis (Building Master JSON)")
    print("─" * 40)
    results["bulk_market_analyzer.py"] = run_script("bulk_market_analyzer.py", "Phase 3", required=True)
    
    if not results["bulk_market_analyzer.py"].ok:
        print("\n🛑 CRITICAL: bulk_market_analyzer.py failed.")
        print("   Cannot produce all_stocks_fundamental_analysis.json.")
        return
    
    # ─── PHASE 4: Enrichment (Order Matters!) ───
    print("\n✨ PHASE 4: Enrichment (Injecting into Master JSON)")
    print("─" * 40)
    
    for script in PHASE4_SCRIPTS:
        results[script] = run_script(script, "Phase 4")
    
    # ─── PHASE 5: Compression ───
    print("\n📦 PHASE 5: Compression (.json → .json.gz)")
    print("─" * 40)
    raw_size, gz_size = compress_output()
    
    # ─── PHASE 6: Optional Standalone Data ───
    if FETCH_OPTIONAL:
        print("\n📋 PHASE 6: Optional Standalone Data")
        print("─" * 40)
        for script in ["fetch_etf_data.py"]:
            results[script] = run_script(script, "Phase 6")
    
    # ─── CLEANUP: Remove intermediate files ───
    if CLEANUP_INTERMEDIATE:
        print("\n🧹 CLEANUP: Removing intermediate files...")
        print("─" * 40)
        cleanup_intermediate()
    
    # ─── FINAL REPORT ───
    total_time = time.time() - overall_start
    success = sum(1 for v in results.values() if v.ok)
    failed = sum(1 for v in results.values() if not v.ok)
    
    print("\n" + "═" * 60)
    print("  PIPELINE COMPLETE")
    print("═" * 60)
    print(f"  Total Time:  {total_time:.1f}s ({total_time/60:.1f} min)")
    print(f"  Successful:  {success}/{len(results)}")
    print(f"  Failed:      {failed}/{len(results)}")
    
    if failed > 0:
        print("\n  Failed Scripts:")
        for script, result in results.items():
            if not result.ok:
                critical = " CRITICAL" if result.required else ""
                print(f"    ❌{critical} {script}")
    
    gz_path = os.path.join(BASE_DIR, "all_stocks_fundamental_analysis.json.gz")
    if os.path.exists(gz_path):
        gz_mb = os.path.getsize(gz_path) / (1024 * 1024)
        raw_mb = raw_size / (1024 * 1024) if raw_size else 0
        print(f"\n  📄 Output: all_stocks_fundamental_analysis.json.gz ({gz_mb:.1f} MB)")
        if raw_size:
            print(f"  📦 Compression: {raw_mb:.1f} MB → {gz_mb:.1f} MB ({(1 - gz_mb/raw_mb)*100:.0f}% smaller)")
    
    if CLEANUP_INTERMEDIATE:
        print(f"  🧹 Only .json.gz + ohlcv_data/ remain. All intermediate data purged.")
    
    print("═" * 60)


if __name__ == "__main__":
    main()
