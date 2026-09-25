"""Publish fetched corporate actions as a traceable, non-adjusted price ledger."""

import os
import sys

from pipeline_utils import BASE_DIR, load_json, save_json


HISTORY_FILE = os.path.join(BASE_DIR, "history_corporate_actions.json")
OUTPUT_FILE = os.path.join(BASE_DIR, "corporate_action_ledger.json")
PRICE_ACTIONS = {"BONUS", "SPLIT", "RIGHTS"}


def build_ledger(actions):
    """Preserve source text; a numeric ratio must come from a verified source."""
    records = []
    seen = set()
    for action in actions:
        symbol = action.get("Symbol")
        action_type = str(action.get("Type") or "").upper()
        ex_date = action.get("ExDate")
        key = (symbol, action_type, ex_date, action.get("RecordDate"), action.get("Details"))
        if not symbol or not ex_date or key in seen:
            continue
        seen.add(key)
        affects_price = action_type in PRICE_ACTIONS
        records.append({
            "symbol": symbol,
            "name": action.get("Name"),
            "action_type": action_type,
            "ex_date": ex_date,
            "record_date": action.get("RecordDate"),
            "source_details": action.get("Details"),
            "affects_price": affects_price,
            "adjustment_factor": None,
            "adjustment_status": "requires_verified_ratio" if affects_price else "not_applicable",
        })
    return sorted(records, key=lambda row: (row["ex_date"], row["symbol"], row["action_type"]))


def main():
    try:
        actions = load_json(HISTORY_FILE)
    except FileNotFoundError:
        print(f"Error: {HISTORY_FILE} is missing.")
        return False
    if not isinstance(actions, list):
        print("Error: corporate-action history is not a list.")
        return False
    ledger = {
        "source": "Dhan ScanX corporate action history",
        "price_adjusted": False,
        "records": build_ledger(actions),
    }
    save_json(OUTPUT_FILE, ledger, ensure_ascii=False)
    print(f"Saved {len(ledger['records'])} corporate-action ledger records.")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
