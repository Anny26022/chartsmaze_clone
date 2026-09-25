"""Build a date-based NSE full-universe delivery-history artifact."""

import argparse
from datetime import datetime, timedelta, timezone

from nse_delivery import fetch_delivery_history_by_date
from pipeline_utils import save_json


def main(argv=None):
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-date", default=(today - timedelta(days=30)).isoformat())
    parser.add_argument("--to-date", default=today.isoformat())
    parser.add_argument("--output", default="delivery_history.json")
    args = parser.parse_args(argv)
    records, skipped = fetch_delivery_history_by_date(args.from_date, args.to_date)
    save_json(args.output, {"source": "NSE daily full bhavcopy and security deliverable data", "from_date": args.from_date, "to_date": args.to_date, "records": records, "skipped_dates": skipped}, ensure_ascii=False)
    print(f"Saved {len(records)} delivery records; skipped {len(skipped)} unavailable calendar dates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
