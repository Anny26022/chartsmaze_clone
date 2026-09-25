"""Maintain a cached 260-session NSE full-universe delivery history."""

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from nse_delivery import NSE_HEADERS, fetch_delivery_file_for_date
from pipeline_utils import BASE_DIR, save_json


CACHE_DIR = Path(BASE_DIR) / "delivery_history_data"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=260)
    parser.add_argument("--max-calendar-days", type=int, default=400)
    args = parser.parse_args(argv)
    if args.sessions <= 0 or args.max_calendar_days < args.sessions:
        parser.error("sessions must be positive and max-calendar-days must cover it")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    fetched = 0
    for offset in range(args.max_calendar_days):
        day = today - timedelta(days=offset)
        path = CACHE_DIR / f"{day.isoformat()}.json"
        if path.exists():
            continue
        rows = fetch_delivery_file_for_date(day, session)
        if rows is not None:
            save_json(path, {"date": day.isoformat(), "records": rows}, ensure_ascii=False)
            fetched += 1
    available = sorted(CACHE_DIR.glob("*.json"), reverse=True)[:args.sessions]
    if len(available) < args.sessions:
        print(f"Only {len(available)}/{args.sessions} delivery sessions are cached.")
        return 1
    print(f"Delivery history ready: {len(available)} sessions ({fetched} downloaded).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
