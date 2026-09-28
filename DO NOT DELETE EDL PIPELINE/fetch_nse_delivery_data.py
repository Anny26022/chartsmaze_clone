"""Stage NSE's newest full-universe delivery bhavcopy for the EDL pipeline."""

from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from nse_delivery import fetch_latest_full_bhavcopy
from pipeline_utils import BASE_DIR, save_json


OUTPUT_FILE = Path(BASE_DIR) / "nse_delivery_data.json"


def main():
    metadata, records, ohlcv_records = fetch_latest_full_bhavcopy()
    save_json(OUTPUT_FILE, {
        **metadata,
        "retrieved_at": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
        "records": records,
        "ohlcv_records": ohlcv_records,
    }, ensure_ascii=False)
    print(
        f"Saved {len(records)} delivery and {len(ohlcv_records)} OHLCV rows from {metadata['file_name']} "
        f"for {metadata['as_of_date']}."
    )
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
