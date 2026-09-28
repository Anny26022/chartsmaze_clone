"""Fetch ScanX filing metadata with a resumable LODR-history backfill.

Every refresh gets page one for freshness. A symbol whose LODR history has
not previously completed is paginated once and persisted in
``filing_history_data``; later refreshes do not re-download old pages.
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from pipeline_utils import ensure_dir, get_headers, load_json, resolve_path, save_json


INPUT_FILE = "master_isin_map.json"
OUTPUT_DIR = "company_filings"
HISTORY_DIR = "filing_history_data"
HISTORY_FILE = f"{HISTORY_DIR}/filing_history.json"
LEGACY_URL = "https://ow-static-scanx.dhan.co/staticscanx/company_filings"
LODR_URL = "https://ow-static-scanx.dhan.co/staticscanx/lodr"
# Historical pagination can make several requests for one company. Keep the
# first backfill deliberately below the old 20-way all-symbol fan-out.
MAX_THREADS = max(1, int(os.getenv("EDL_FILINGS_MAX_THREADS", "8")))
PAGE_SIZE = 100


def fetch_page(url, isin, headers, page=1):
    """Return records, endpoint page count and an error string when unavailable."""
    payload = {"data": {"isin": isin, "pg_no": page, "count": PAGE_SIZE}}
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as error:
        return None, None, str(error)
    records = payload.get("data")
    if not isinstance(records, list):
        return None, None, "response data is not a list"
    try:
        pages = max(1, int(payload.get("total_pages")))
    except (TypeError, ValueError):
        pages = 1
    return records, pages, None


def _key(entry):
    news_id = entry.get("news_id")
    if news_id not in (None, ""):
        return f"id:{news_id}"
    return "|".join(str(entry.get(field) or "") for field in ("news_date", "descriptor", "caption", "file_url"))


def _rank(entry):
    return (bool(entry.get("file_url")), bool(entry.get("news_body")), len(str(entry.get("caption") or "")))


def dedupe_filings(items):
    """Deduplicate endpoint/page overlap while preferring the fuller record."""
    unique = {}
    for raw in items:
        if not isinstance(raw, dict):
            continue
        entry = dict(raw)
        key = _key(entry)
        if not key.strip("|"):
            continue
        if key not in unique or _rank(entry) > _rank(unique[key]):
            unique[key] = entry
    return sorted(unique.values(), key=lambda item: str(item.get("news_date") or ""), reverse=True)


def _with_source(records, endpoint):
    return [{**record, "source_endpoint": endpoint} for record in records if isinstance(record, dict)]


def _history_entry(existing, isin, legacy, lodr, total_pages, completed):
    previous = existing if isinstance(existing, dict) else {}
    return {
        "isin": isin,
        "lodr_total_pages": total_pages,
        "lodr_backfill_complete": completed,
        "filings": dedupe_filings([*(previous.get("filings") or []), *legacy, *lodr]),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_filings(item, existing_history=None):
    """Fetch current pages and any missing historical LODR pages for one ISIN."""
    symbol = str(item.get("Symbol") or "").upper()
    isin = item.get("ISIN")
    if not symbol or not isin:
        return {"symbol": symbol, "status": "error", "error": "missing symbol or ISIN"}

    headers = get_headers(include_origin=True)
    legacy_page, _legacy_pages, legacy_error = fetch_page(LEGACY_URL, isin, headers)
    lodr_page, total_pages, lodr_error = fetch_page(LODR_URL, isin, headers)
    existing_history = existing_history or {}
    legacy = _with_source(legacy_page or [], "company_filings")
    lodr = _with_source(lodr_page or [], "lodr")
    completed = bool(existing_history.get("lodr_backfill_complete"))

    if lodr_page is not None and not completed:
        completed = True
        for page in range(2, total_pages + 1):
            records, _pages, error = fetch_page(LODR_URL, isin, headers, page)
            if records is None:
                completed = False
                lodr_error = error
                break
            lodr.extend(_with_source(records, "lodr"))

    # If page one fails after a completed backfill, retain known history rather
    # than turning a temporary provider failure into data loss.
    if lodr_page is None:
        total_pages = existing_history.get("lodr_total_pages", 1)
    history = _history_entry(existing_history, isin, legacy, lodr, total_pages or 1, completed)
    current = dedupe_filings([*legacy, *(_with_source(lodr_page or [], "lodr"))]) or history["filings"]
    return {
        "symbol": symbol,
        "status": "success" if legacy_page is not None or lodr_page is not None else "error",
        "current": current,
        "history": history,
        "error": legacy_error or lodr_error,
    }


def _load_history():
    payload = load_json(HISTORY_FILE, default={})
    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    return symbols if isinstance(symbols, dict) else {}


def _save_history(history):
    completed = sum(bool(record.get("lodr_backfill_complete")) for record in history.values())
    save_json(HISTORY_FILE, {
        "schema_version": 1,
        "source": "ScanX static company_filings and LODR endpoints",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": history,
        "coverage": {"symbols": len(history), "lodr_backfill_complete": completed, "lodr_backfill_pending": len(history) - completed},
    }, ensure_ascii=False)


def main():
    ensure_dir(OUTPUT_DIR)
    ensure_dir(HISTORY_DIR)
    try:
        stock_list = load_json(INPUT_FILE)
    except Exception as error:
        print(f"Error loading {INPUT_FILE}: {error}")
        return False
    if not isinstance(stock_list, list) or not stock_list:
        print("No canonical symbols available for filings fetch.")
        return False

    canonical_symbols = {str(item.get("Symbol") or "").upper() for item in stock_list}
    # Do not carry a delisted/SME symbol forward merely because it existed in
    # an older cache. The published ledger follows the canonical universe.
    existing = {symbol: record for symbol, record in _load_history().items() if symbol in canonical_symbols}
    history = dict(existing)
    results = []
    started = time.time()
    print(f"Refreshing page one for {len(stock_list)} mainboard symbols; threads: {MAX_THREADS}.")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(fetch_filings, item, existing.get(str(item.get("Symbol") or "").upper())): item
            for item in stock_list
        }
        for count, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            results.append(result)
            if result.get("history"):
                history[result["symbol"]] = result["history"]
            if count % 100 == 0 or count == len(futures):
                # A first historical sweep can outlast the enclosing stage's
                # timeout. Persist completed symbols so the next run resumes.
                _save_history(history)
                print(f"[{count}/{len(futures)}] elapsed {time.time() - started:.1f}s")

    for result in results:
        if result.get("current"):
            save_json(resolve_path(OUTPUT_DIR) / f"{result['symbol']}_filings.json", {"code": 0, "data": result["current"]})

    _save_history(history)
    completed = sum(bool(record.get("lodr_backfill_complete")) for record in history.values())
    succeeded = sum(result.get("status") == "success" for result in results)
    print(f"Filings refreshed: {succeeded}/{len(results)}; LODR histories complete: {completed}/{len(history)}.")
    return succeeded > 0


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
