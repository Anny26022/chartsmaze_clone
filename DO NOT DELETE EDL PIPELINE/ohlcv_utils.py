import csv
from datetime import datetime


OHLCV_FIELDS = ["Date", "Open", "High", "Low", "Close", "Volume"]


def date_string(value):
    return value if isinstance(value, str) else datetime.fromtimestamp(value).strftime("%Y-%m-%d")


def rows_from_tick_data(data):
    times = data.get("Time", [])
    if not times:
        return []

    opens = data.get("o", [])
    highs = data.get("h", [])
    lows = data.get("l", [])
    closes = data.get("c", [])
    volumes = data.get("v", [])

    rows = []
    for index, timestamp in enumerate(times):
        rows.append({
            "Date": date_string(timestamp),
            "Open": opens[index],
            "High": highs[index],
            "Low": lows[index],
            "Close": closes[index],
            "Volume": volumes[index],
        })
    return rows


def read_ohlcv_csv(path):
    try:
        with open(path, "r") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def merge_rows_by_date(rows):
    return sorted({row["Date"]: row for row in rows}.values(), key=lambda row: row["Date"])


def write_ohlcv_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OHLCV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
