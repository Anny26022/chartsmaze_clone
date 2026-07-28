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


def plan_history_ranges(existing_rows, desired_start_ts, desired_end_ts):
    """Plan backward and forward gaps without discarding an existing cache."""
    if desired_start_ts >= desired_end_ts:
        return []
    if not existing_rows:
        return [(int(desired_start_ts), int(desired_end_ts))]

    parsed = []
    for row in existing_rows:
        try:
            parsed.append(int(datetime.strptime(row["Date"], "%Y-%m-%d").timestamp()))
        except (KeyError, TypeError, ValueError):
            continue
    if not parsed:
        return [(int(desired_start_ts), int(desired_end_ts))]

    one_day = 86400
    first_ts = min(parsed)
    last_ts = max(parsed)
    ranges = []
    if desired_start_ts < first_ts - one_day:
        ranges.append((int(desired_start_ts), int(first_ts - one_day)))
    if last_ts + one_day < desired_end_ts:
        ranges.append((int(last_ts + one_day), int(desired_end_ts)))
    return ranges


def chunk_history_range(start_ts, end_ts, chunk_days):
    """Yield non-overlapping API ranges from newest to oldest."""
    if chunk_days <= 0:
        raise ValueError("chunk_days must be positive")
    chunks = []
    pointer = int(end_ts)
    span = int(chunk_days * 86400)
    while pointer > start_ts:
        chunk_start = max(int(start_ts), pointer - span)
        chunks.append((chunk_start, pointer))
        pointer = chunk_start
    return chunks


def write_ohlcv_csv(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OHLCV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
