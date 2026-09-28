"""Fetch NSE's official current F&O security-ban report."""

from pathlib import Path

from nse_fno_ban import fetch_latest_fno_ban
from pipeline_utils import BASE_DIR, save_json


def main():
    try:
        data = fetch_latest_fno_ban()
    except Exception as error:
        # An unavailable report is not an empty ban: preserve that distinction
        # so rules return unavailable rather than falsely including a security.
        data = {
            "source": "NSE daily F&O Security in Ban Period report",
            "available": False,
            "trade_date": None,
            "symbols": [],
            "error": str(error),
        }
        print(f"Warning: F&O-ban report unavailable: {error}")
    save_json(Path(BASE_DIR) / "nse_fno_ban.json", data, ensure_ascii=False)
    print(f"Saved {len(data['symbols'])} F&O-ban symbols for trade date {data['trade_date']} (available={data['available']}).")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
