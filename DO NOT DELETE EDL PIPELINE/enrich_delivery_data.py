"""Merge the newest staged NSE delivery record into each stock snapshot."""

from pathlib import Path

from pipeline_utils import BASE_DIR, load_json, save_json


MASTER_FILE = Path(BASE_DIR) / "all_stocks_fundamental_analysis.json"
DELIVERY_FILE = Path(BASE_DIR) / "nse_delivery_data.json"


def latest_by_symbol(records: list[dict]) -> dict[str, dict]:
    latest = {}
    for record in records:
        if not isinstance(record, dict) or not record.get("symbol") or not record.get("date"):
            continue
        symbol = str(record["symbol"]).upper()
        if symbol not in latest or record["date"] > latest[symbol]["date"]:
            latest[symbol] = record
    return latest


def apply_delivery_data(stocks: list[dict], records: list[dict]) -> int:
    by_symbol = latest_by_symbol(records)
    applied = 0
    for stock in stocks:
        record = by_symbol.get(str(stock.get("Symbol") or "").upper())
        if not record:
            continue
        stock["Delivery %"] = record["delivery_percent"]
        stock["Deliverable Quantity"] = record["deliverable_quantity"]
        stock["Delivery Traded Quantity"] = record["traded_quantity"]
        stock["Delivery As Of Date"] = record["date"]
        stock["Delivery Series"] = record.get("series")
        applied += 1
    return applied


def main():
    if not MASTER_FILE.exists():
        print(f"Error: {MASTER_FILE} not found.")
        return False
    if not DELIVERY_FILE.exists():
        print("No staged NSE delivery data; retaining null delivery fields.")
        return True
    payload = load_json(DELIVERY_FILE)
    records = payload.get("records", []) if isinstance(payload, dict) else []
    stocks = load_json(MASTER_FILE)
    applied = apply_delivery_data(stocks, records)
    save_json(MASTER_FILE, stocks, ensure_ascii=False)
    print(f"Merged NSE delivery data into {applied}/{len(stocks)} stocks.")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
