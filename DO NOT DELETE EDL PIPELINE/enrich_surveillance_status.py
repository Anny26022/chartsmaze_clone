"""Attach the current ASM/GSM surveillance status to canonical stock rows."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from pipeline_utils import load_json, save_json


MASTER_FILE = "all_stocks_fundamental_analysis.json"
ASM_FILE = "nse_asm_list.json"
GSM_FILE = "nse_gsm_list.json"


def _symbol(value):
    return str(value or "").strip().upper()


def _stage_map(path):
    """Return ``None`` when a list was not retrieved; an empty list is valid."""
    try:
        records = load_json(path)
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(records, list):
        return None
    return {
        symbol: (str(row.get("Stage")).strip() or None)
        for row in records
        if isinstance(row, dict) and (symbol := _symbol(row.get("Symbol") or row.get("symbol")))
    }


def apply_surveillance_status(stocks, asm_stages, gsm_stages, fetched_at):
    """Mutate stock rows with a complete, conservative surveillance snapshot."""
    lists_available = asm_stages is not None and gsm_stages is not None
    for stock in stocks:
        as_of_date = str(stock.get("as_of_date") or "").strip() or None
        available = bool(lists_available and as_of_date)
        symbol = _symbol(stock.get("Symbol") or stock.get("symbol"))
        stock["surveillance_available"] = available
        stock["surveillance_as_of_date"] = as_of_date if available else None
        stock["surveillance_fetched_at"] = fetched_at if available else None
        stock["is_asm"] = symbol in asm_stages if available else None
        stock["asm_stage"] = asm_stages.get(symbol) if available else None
        stock["is_gsm"] = symbol in gsm_stages if available else None
        stock["gsm_stage"] = gsm_stages.get(symbol) if available else None
    return lists_available


def main():
    stocks = load_json(MASTER_FILE)
    if not isinstance(stocks, list):
        raise ValueError(f"{MASTER_FILE} must contain a stock list")
    asm_stages, gsm_stages = _stage_map(ASM_FILE), _stage_map(GSM_FILE)
    fetched_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    available = apply_surveillance_status(stocks, asm_stages, gsm_stages, fetched_at)
    save_json(MASTER_FILE, stocks)
    if available:
        asm_count = sum(bool(stock["is_asm"]) for stock in stocks)
        gsm_count = sum(bool(stock["is_gsm"]) for stock in stocks)
        print(f"Surveillance status: ASM={asm_count}, GSM={gsm_count}; fetched {fetched_at}")
    else:
        print("Surveillance status unavailable: ASM or GSM list was not retrieved; all rows marked unavailable.")
    return True


if __name__ == "__main__":
    main()
