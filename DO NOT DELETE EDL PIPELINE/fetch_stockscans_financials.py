"""Fetch verified consolidated quarterly fallbacks from StockScans.

This source is intentionally a fallback for incomplete or stale ScanX
*consolidated* statements.  A statement is eligible only when its latest
period can be tied to a StockScans Result document and its announcement.  The
tie prevents a current page scrape from leaking future results into an
as-of-date scanner snapshot.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import os
import time

import requests

from pipeline_utils import load_json, save_json


MASTER_FILE = "master_isin_map.json"
FUNDAMENTAL_FILE = "fundamental_data.json"
OUTPUT_FILE = "stockscans_financial_data.json"
STATEMENT_URL = "https://www.stockscans.in/api/company/fundamentals/statements/{company_id}/C"
DOCUMENTS_URL = "https://www.stockscans.in/api/company/fundamentals/documents/{company_id}"
ANNOUNCEMENTS_URL = "https://www.stockscans.in/api/company/fundamentals/announcements"
MAX_WORKERS = int(os.getenv("STOCKSCANS_MAX_WORKERS", "4"))
REQUEST_DELAY_SECONDS = float(os.getenv("STOCKSCANS_REQUEST_DELAY_SECONDS", "0.15"))
ANNOUNCEMENT_BATCH_SIZE = int(os.getenv("STOCKSCANS_ANNOUNCEMENT_BATCH_SIZE", "5"))
MAX_ANNOUNCEMENT_PAGES = int(os.getenv("STOCKSCANS_MAX_ANNOUNCEMENT_PAGES", "50"))


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def headers():
    return {"Accept": "application/json", "User-Agent": "Chartsmaze data pipeline/1.0"}


def get_json(url):
    response = requests.get(url, headers=headers(), timeout=20)
    response.raise_for_status()
    return response.json()


def post_json(url, payload):
    response = requests.post(url, json=payload, headers=headers(), timeout=20)
    response.raise_for_status()
    return response.json()


def number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def complete_scanx_consolidated(item):
    statement = (item or {}).get("incomeStat_cq") or {}
    return all(str(statement.get(field) or "").split("|")[0] not in {"", "None"}
               for field in ("SALES", "NET_PROFIT", "EPS"))


def latest_scanx_period(item):
    value = str(((item or {}).get("incomeStat_cq") or {}).get("YEAR") or "").split("|")[0]
    return value if len(value) == 6 and value.isdigit() else None


def latest_completed_quarter_period(now=None):
    now = now or datetime.now(timezone.utc)
    # In Jul-Sep the latest completed quarter is June; in Oct-Dec it is Sep.
    latest_month = ((now.month - 1) // 3) * 3
    if latest_month == 0:
        return f"{now.year - 1}12"
    return f"{now.year}{latest_month:02d}"


def needs_stockscans_fallback(item, now=None):
    return (not complete_scanx_consolidated(item)
            or (latest_scanx_period(item) or "") < latest_completed_quarter_period(now))


def normalize_statement(company_id, payload):
    rows = payload.get("quarterly") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or len(rows) < 2 or not isinstance(rows[0], list):
        return []
    columns = {name: index for index, name in enumerate(rows[0])}
    required = ("Date", "Revenue", "PAT", "EPS")
    if any(name not in columns for name in required):
        return []
    normalized = []
    for row in rows[1:]:
        if not isinstance(row, list):
            continue
        period = str(row[columns["Date"]]) if columns["Date"] < len(row) else ""
        if len(period) != 6 or not period.isdigit():
            continue
        item = {"period": period}
        for source, target in (("Revenue", "sales"), ("Operating Profit", "operating_profit"),
                               ("PBT", "pbt"), ("PAT", "net_profit"), ("EPS", "eps"),
                               ("OPM", "opm")):
            index = columns.get(source)
            item[target] = number(row[index]) if index is not None and index < len(row) else None
        normalized.append(item)
    return sorted(normalized, key=lambda entry: entry["period"], reverse=True)


def result_documents(company_id):
    payload = get_json(DOCUMENTS_URL.format(company_id=company_id))
    result = {}
    for document in payload.get("documents", []):
        period = str(document.get("date") or "")
        url = document.get("ssUrl")
        if document.get("documentType") == "Result" and len(period) == 6 and period.isdigit() and url:
            result[period] = str(url)
    return result


def announcement_dates(company_ids, result_urls):
    """Find the announcement date for Result PDFs using bounded public pages.

    The endpoint accepts a company-id array but pages results globally.  Stop
    once every required Result document is found; never infer a date from a
    presentation, transcript, or board-meeting intimation.
    """
    pending = {url for urls in result_urls.values() for url in urls.values()}
    found = {}
    offset = 0
    for _page in range(MAX_ANNOUNCEMENT_PAGES):
        if not pending:
            break
        try:
            payload = post_json(ANNOUNCEMENTS_URL, {"companyIds": company_ids, "offset": offset})
        except (requests.RequestException, ValueError):
            # A failed announcement batch must leave its records unverified,
            # not discard successful statements from other batches.
            break
        rows = payload.get("companyAnnouncements", [])
        if not isinstance(rows, list) or not rows:
            break
        for announcement in rows:
            url = str(announcement.get("ssUrl") or "")
            date = str(announcement.get("date") or "")
            if url in pending and len(date) == 10:
                found[url] = date
                pending.remove(url)
        limit = payload.get("limit")
        if not isinstance(limit, int) or limit <= 0 or len(rows) < limit:
            break
        offset += limit
        time.sleep(REQUEST_DELAY_SECONDS)
    return found


def fetch_statement_and_documents(item):
    symbol = item["Symbol"]
    company_id = f"NSE:{symbol}"
    encoded_company_id = requests.utils.quote(company_id, safe="")
    try:
        statement = normalize_statement(company_id, get_json(STATEMENT_URL.format(company_id=encoded_company_id)))
        documents = result_documents(encoded_company_id)
        return symbol, {"statement": statement, "documents": documents}
    except (requests.RequestException, ValueError):
        return symbol, None


def fetch_stockscans_financials():
    master = load_json(MASTER_FILE)
    scanx_by_symbol = {row.get("Symbol"): row for row in load_json(FUNDAMENTAL_FILE) if row.get("Symbol")}
    candidates = [row for row in master if row.get("Symbol") and needs_stockscans_fallback(scanx_by_symbol.get(row["Symbol"]))]
    print(f"StockScans fallback candidates: {len(candidates)} incomplete or stale ScanX consolidated statements.")

    raw = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_statement_and_documents, item) for item in candidates]
        for index, future in enumerate(as_completed(futures), start=1):
            symbol, result = future.result()
            if result:
                raw[symbol] = result
            if index % 100 == 0 or index == len(candidates):
                print(f"  statements/documents: {index}/{len(candidates)}")
            time.sleep(REQUEST_DELAY_SECONDS)

    by_symbol = {row["Symbol"]: row for row in candidates}
    result_urls = {
        f"NSE:{symbol}": result["documents"]
        for symbol, result in raw.items() if result["documents"]
    }
    dates_by_url = {}
    company_ids = list(result_urls)
    for start in range(0, len(company_ids), ANNOUNCEMENT_BATCH_SIZE):
        batch = company_ids[start:start + ANNOUNCEMENT_BATCH_SIZE]
        dates_by_url.update(announcement_dates(batch, {company_id: result_urls[company_id] for company_id in batch}))
        time.sleep(REQUEST_DELAY_SECONDS)

    records = {}
    for symbol, result in raw.items():
        documents = result["documents"]
        dates = {period: dates_by_url[url] for period, url in documents.items() if url in dates_by_url}
        latest_period = result["statement"][0]["period"] if result["statement"] else None
        if not latest_period or latest_period not in dates:
            continue
        master_row = by_symbol[symbol]
        records[symbol] = {
            "symbol": symbol,
            "isin": master_row.get("ISIN"),
            "company_id": f"NSE:{symbol}",
            "statement_type": "CONSOLIDATED",
            "quarterly": result["statement"],
            "announcement_dates": dates,
            "latest_result_date": dates[latest_period],
            "latest_period": latest_period,
            "source": "STOCKSCANS_PUBLIC",
            "source_url": STATEMENT_URL.format(company_id=requests.utils.quote(f"NSE:{symbol}", safe="")),
            "retrieved_at": now_iso(),
        }

    save_json(OUTPUT_FILE, {
        "source": "StockScans public company fundamentals API",
        "generated_at": now_iso(),
        "records": records,
        "quality": {
            "candidates": len(candidates),
            "statement_or_document_responses": len(raw),
            "verified_latest_results": len(records),
            "unverified_or_unavailable": len(candidates) - len(records),
        },
    })
    print(f"StockScans verified consolidated fallbacks: {len(records)}/{len(candidates)}")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if fetch_stockscans_financials() else 1)
