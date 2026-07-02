from datetime import datetime, timedelta
import io

import pandas as pd
import requests


NSE_ARCHIVE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "*/*",
}


def clean_records(df):
    records = []
    for record in df.to_dict(orient="records"):
        records.append({
            key.strip() if isinstance(key, str) else key: value.strip() if isinstance(value, str) else value
            for key, value in record.items()
        })
    return records


def fetch_latest_nse_csv(base_url, label, lookback_days=8, timeout=15):
    """Return records from the newest NSE archive CSV found within the lookback window."""
    today = datetime.now()

    for offset in range(lookback_days):
        check_date = today - timedelta(days=offset)
        date_str = check_date.strftime("%d%m%Y")
        url = base_url.format(date=date_str)

        print(f"Checking for {label} on {date_str}...")
        try:
            response = requests.get(url, headers=NSE_ARCHIVE_HEADERS, timeout=timeout)
            if response.status_code == 404:
                print(f"  No file found for {date_str} (404).")
                continue
            if response.status_code != 200:
                print(f"  Unexpected status {response.status_code} for {date_str}.")
                continue

            print(f"  Found data for {date_str}!")
            df = pd.read_csv(io.StringIO(response.content.decode("utf-8")))
            return clean_records(df)
        except Exception as e:
            print(f"  Error checking {date_str}: {e}")

    return []
