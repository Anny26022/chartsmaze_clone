"""Maintain a local 260-session NSE full-universe delivery-history cache."""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from nse_delivery import NSE_HEADERS, fetch_delivery_file_for_date
from pipeline_utils import BASE_DIR, save_json


DEFAULT_SESSIONS = 260  # Covers the scanner's 252-session / 52-week rules.
RECENT_RECHECK_DAYS = 7
MANIFEST_NAME = "_availability.json"


def _manifest_path(cache_dir: Path) -> Path:
    return cache_dir / MANIFEST_NAME


def _load_manifest(cache_dir: Path) -> dict:
    path = _manifest_path(cache_dir)
    if not path.exists():
        return {"not_published": []}
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        return {"not_published": []}
    dates = payload.get("not_published", []) if isinstance(payload, dict) else []
    return {"not_published": sorted({str(value) for value in dates})}


def _save_manifest(cache_dir: Path, manifest: dict) -> None:
    save_json(_manifest_path(cache_dir), {"not_published": sorted(set(manifest["not_published"]))})


def _is_valid_session(path: Path) -> bool:
    """A cache file counts only when it is a non-empty, date-aligned payload."""
    try:
        payload = json.loads(path.read_text())
    except (OSError, ValueError):
        return False
    records = payload.get("records") if isinstance(payload, dict) else None
    return (
        isinstance(records, list)
        and bool(records)
        and payload.get("date") == path.stem
        and any(item.get("date") == path.stem for item in records if isinstance(item, dict))
    )


def cached_sessions(cache_dir: Path) -> list[Path]:
    return sorted(
        (path for path in cache_dir.glob("????-??-??.json") if _is_valid_session(path)),
        reverse=True,
    )


def backfill_delivery_history(
    cache_dir: Path,
    sessions: int = DEFAULT_SESSIONS,
    max_calendar_days: int = 400,
    today=None,
    fetcher=fetch_delivery_file_for_date,
) -> tuple[int, int, list[str]]:
    """Fill a bounded trading-session cache and return downloaded, available, failures.

    A 404 is a normal non-published day.  It is remembered after a short grace
    period, while recent dates are rechecked because NSE can publish a daily file
    after market close.  Transport/server failures are never recorded as holidays.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    today = today or datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    manifest = _load_manifest(cache_dir)
    unavailable = set(manifest["not_published"])
    available_dates = {path.stem for path in cached_sessions(cache_dir)}
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    downloaded, failures = 0, []

    for offset in range(max_calendar_days):
        if len(available_dates) >= sessions:
            break
        day = today - timedelta(days=offset)
        day_key = day.isoformat()
        path = cache_dir / f"{day_key}.json"
        if day_key in available_dates:
            continue
        if day_key in unavailable and (today - day).days > RECENT_RECHECK_DAYS:
            continue
        try:
            rows = fetcher(day, session)
        except requests.RequestException as error:
            failures.append(f"{day_key}: {error}")
            continue
        if rows is None:
            if (today - day).days > RECENT_RECHECK_DAYS:
                unavailable.add(day_key)
            continue
        if not rows or not any(row.get("date") == day_key for row in rows):
            failures.append(f"{day_key}: NSE returned no date-aligned delivery rows")
            continue
        save_json(path, {"date": day_key, "records": rows}, ensure_ascii=False)
        available_dates.add(day_key)
        unavailable.discard(day_key)
        downloaded += 1

    _save_manifest(cache_dir, {"not_published": sorted(unavailable)})
    return downloaded, len(available_dates), failures


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--max-calendar-days", type=int, default=400)
    parser.add_argument("--cache-dir", type=Path, default=Path(BASE_DIR) / "delivery_history_data")
    args = parser.parse_args(argv)
    if args.sessions <= 0 or args.max_calendar_days < args.sessions:
        parser.error("sessions must be positive and max-calendar-days must cover it")
    fetched, available, failures = backfill_delivery_history(
        args.cache_dir, args.sessions, args.max_calendar_days,
    )
    if failures:
        print("NSE delivery fetch failures:\n" + "\n".join(failures[:10]))
    if available < args.sessions:
        print(f"Only {available}/{args.sessions} delivery sessions are cached.")
        return 1
    print(f"Delivery history ready: {available} sessions ({fetched} downloaded).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
