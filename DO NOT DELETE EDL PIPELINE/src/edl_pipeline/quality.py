"""Whole-publication integrity and coverage checks, independent of the UI."""
from collections import Counter
import csv
from datetime import date, datetime, timedelta, timezone
import gzip
import os

from .validators import strict_json_load


def read_json(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return strict_json_load(handle)


def ohlc_error(row):
    values = [row.get(key) for key in ("open", "high", "low", "close")]
    present = [x for x in values if x is not None]
    if any(type(x) not in (int, float) or x <= 0 for x in present):
        return "non-positive or non-numeric OHLC"
    if len(present) == 4:
        opening, high, low, close = values
        if not low <= min(opening, close) <= max(opening, close) <= high:
            return "inconsistent OHLC"
    volume = row.get("volume")
    if volume is not None and (type(volume) not in (int, float) or volume < 0):
        return "invalid volume"
    return None


def inspect_publication(root, today=None, expected_session=None, max_age_days=None):
    today = today or datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    max_age_days = max_age_days if max_age_days is not None else int(os.getenv("EDL_MAX_SESSION_AGE_DAYS", "7"))
    errors = []
    try:
        stocks = read_json(root / "all_stocks_fundamental_analysis.json.gz")
        indices = read_json(root / "all_indices_history_v2.json.gz")
        breadth = read_json(root / "market_breadth_v2.json.gz")
        universe = read_json(root / "breadth_universe_snapshot.json.gz")
        source = read_json(root / "master_isin_map.json")
        for name in ("sector_analytics.json.gz", "all_indices_list.json"):
            read_json(root / name)
        benchmark = next(x for x in indices["indices"] if x["symbol"] == "NIFTY")
        session = date.fromisoformat(benchmark["records"][-1]["date"])
        expected_session = expected_session or os.getenv("EDL_EXPECTED_SESSION")
        if expected_session and session.isoformat() != expected_session:
            errors.append(f"benchmark session {session} != expected {expected_session}")
        if not 0 <= (today - session).days <= max_age_days:
            errors.append(f"benchmark session outside freshness limit: {session}")
        generated = [x["generated_at"] for x in (indices, breadth, universe)]
        if len(set(generated)) != 1 or any(datetime.fromisoformat(x).astimezone(timezone(timedelta(hours=5, minutes=30))).date() != today for x in generated):
            errors.append("v2 outputs were not generated together today")
        if breadth["records"][-1]["date"] != session.isoformat():
            errors.append("breadth and benchmark sessions differ")
        with gzip.open(root / "market_breadth.json.gz", "rt", encoding="utf-8") as handle:
            legacy = list(csv.reader(handle))
        legacy_dates = [date.fromisoformat(value) for value in legacy[0][1:]]
        if not legacy_dates or legacy_dates != sorted(set(legacy_dates)) or legacy_dates[-1] != session:
            errors.append("legacy breadth dates differ from benchmark")
        if any(len(row) != len(legacy[0]) for row in legacy[1:]):
            errors.append("legacy breadth has inconsistent row widths")
        symbols = [x["symbol"] for x in stocks]
        expected = {x["Symbol"] for x in source}
        if set(symbols) != expected or len(symbols) != len(set(symbols)):
            errors.append("stock universe differs from fetched master or has duplicates")
        availability = []
        for stock in stocks:
            symbol = stock["symbol"]
            missing = sorted(k for k, value in stock.items() if value is None)
            stamp = stock.get("as_of_date")
            if stamp is not None:
                parsed = date.fromisoformat(stamp)
                if parsed > today:
                    errors.append(f"{symbol}: future as_of_date")
            invalid = ohlc_error(stock)
            if invalid:
                errors.append(f"{symbol}: {invalid}")
            availability.append({"symbol": symbol, "as_of_date": stamp,
                                 "history_current": stamp == session.isoformat(),
                                 "missing_fields": missing})
        current = sum(row["history_current"] for row in availability)
        if not stocks or current / len(stocks) < 0.90:
            errors.append(f"current stock-history coverage below 90%: {current}/{len(stocks)}")
        index_availability = []
        for item in indices["indices"]:
            dates = []
            for record in item["records"]:
                stamp = date.fromisoformat(record["date"])
                dates.append(stamp)
                invalid = ohlc_error(record)
                if invalid or stamp > today:
                    errors.append(f"index {item['symbol']} {stamp}: {invalid or 'future date'}")
            if dates != sorted(set(dates)):
                errors.append(f"index {item['symbol']}: unsorted/duplicate dates")
            index_availability.append({"symbol": item["symbol"], "index_id": item.get("index_id"),
                                       "as_of_date": dates[-1].isoformat() if dates else None})
        index_current = sum(x["as_of_date"] == session.isoformat() for x in index_availability)
        if not index_availability or index_current / len(index_availability) < 0.90:
            errors.append("current index-history coverage below 90%")
        return {"reference_session": session.isoformat(),
                "freshness_policy": {"expected_session": expected_session, "max_calendar_age_days": max_age_days,
                                     "note": "Benchmark-aligned; not an exchange-holiday calendar. Set EDL_EXPECTED_SESSION for an exact session gate."},
                "stock_count": len(stocks), "current_history_count": current,
                "missing_field_counts": dict(Counter(k for row in availability for k in row["missing_fields"])),
                "symbols": availability, "indices": index_availability, "errors": errors}
    except (ValueError, KeyError, TypeError, IndexError, StopIteration, OSError) as error:
        return {"errors": [f"invalid publication: {error}"]}
