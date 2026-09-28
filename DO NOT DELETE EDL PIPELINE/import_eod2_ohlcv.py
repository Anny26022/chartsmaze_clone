"""Optionally seed the local OHLCV cache from a checked-out EOD2 data repo.

EOD2 is a historical bootstrap only.  The normal ScanX/NSE sync continues to
own newer candles.  Matching by ISIN (rather than filenames or current ticker)
also keeps renamed securities attached to their own history.
"""

import csv
import json
import os
from datetime import date
from pathlib import Path

from ohlcv_utils import merge_rows_by_date, read_ohlcv_csv, symbol_csv_path, write_ohlcv_csv
from pipeline_utils import BASE_DIR, load_json, save_json


MASTER_FILE = "master_isin_map.json"
REPORT_FILE = "eod2_ohlcv_import_report.json"


def resolve_eod2_data_dir(value):
    """Accept either the eod2_data checkout or an EOD2 checkout containing it."""
    if not value:
        return None
    root = Path(value).expanduser()
    for candidate in (root, root / "src" / "eod2_data"):
        if (candidate / "daily").is_dir() and (candidate / "isin_symbol_map.json").is_file():
            return candidate
    raise ValueError(f"EDL_EOD2_DATA_DIR is not an EOD2 data checkout: {root}")


def finite_number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


def source_rows(path, start_date, end_date):
    """Read only valid OHLCV rows inside an ISIN's recorded symbol interval."""
    rows = []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            for item in csv.DictReader(handle):
                try:
                    session = date.fromisoformat(str(item.get("Date", "")))
                except ValueError:
                    continue
                if session < start_date or session > end_date:
                    continue
                values = {field: finite_number(item.get(field)) for field in ("Open", "High", "Low", "Close", "Volume")}
                opening, high, low, close, volume = (values[field] for field in ("Open", "High", "Low", "Close", "Volume"))
                if (
                    None in values.values() or volume < 0 or low <= 0
                    or not low <= min(opening, close) <= max(opening, close) <= high
                ):
                    continue
                rows.append({"Date": session.isoformat(), **values})
    except OSError:
        return []
    return rows


def eod2_rows_for_isin(data_dir, history, isin):
    """Collect renamed-file segments for one ISIN, with later segments winning."""
    rows = []
    for item in history.get(isin, []):
        if not isinstance(item, dict) or not item.get("symbol"):
            continue
        try:
            start_date = date.fromisoformat(item["from_date"])
            end_date = date.fromisoformat(item["to_date"])
        except (KeyError, TypeError, ValueError):
            continue
        path = data_dir / "daily" / f"{str(item['symbol']).lower()}.csv"
        rows.extend(source_rows(path, start_date, end_date))
    return merge_rows_by_date(rows)


def import_eod2_ohlcv(data_dir, master, output_dir):
    """Overlay adjusted EOD2 history and retain any newer local provider rows."""
    mapping = json.loads((data_dir / "isin_symbol_map.json").read_text(encoding="utf-8"))
    history = mapping.get("isin2hist", {}) if isinstance(mapping, dict) else {}
    if not isinstance(history, dict):
        raise ValueError("EOD2 ISIN history map is malformed")

    output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "enabled": True,
        "source": "EOD2 adjusted daily CSV bootstrap",
        "source_last_update": None,
        "master_symbols": len(master),
        "imported_symbols": 0,
        "imported_rows": 0,
        "unmapped_isins": 0,
        "empty_or_invalid_sources": 0,
    }
    try:
        report["source_last_update"] = json.loads((data_dir / "meta.json").read_text(encoding="utf-8")).get("lastUpdate")
    except (OSError, ValueError, AttributeError):
        pass

    for item in master:
        symbol, isin = item.get("Symbol"), item.get("ISIN")
        if not symbol or not isin:
            continue
        if isin not in history:
            report["unmapped_isins"] += 1
            continue
        imported = eod2_rows_for_isin(data_dir, history, isin)
        if not imported:
            report["empty_or_invalid_sources"] += 1
            continue
        destination = symbol_csv_path(output_dir, symbol)
        # Imported rows intentionally come last: when EOD2 republishes a split
        # adjustment it replaces the overlapping historical rows.  Any local
        # sessions newer than EOD2's weekly snapshot remain in place.
        merged = merge_rows_by_date([*read_ohlcv_csv(destination), *imported])
        write_ohlcv_csv(destination, merged)
        report["imported_symbols"] += 1
        report["imported_rows"] += len(imported)
    return report


def main():
    try:
        data_dir = resolve_eod2_data_dir(os.getenv("EDL_EOD2_DATA_DIR"))
        if data_dir is None:
            save_json(REPORT_FILE, {
                "enabled": False,
                "source": "EOD2 adjusted daily CSV bootstrap",
                "reason": "EDL_EOD2_DATA_DIR is not set",
            })
            print("EOD2 OHLCV bootstrap skipped (EDL_EOD2_DATA_DIR is not set).")
            return True
        report = import_eod2_ohlcv(data_dir, load_json(MASTER_FILE), Path(BASE_DIR) / "ohlcv_data")
        save_json(REPORT_FILE, report)
        print(
            "EOD2 OHLCV bootstrap: "
            f"{report['imported_symbols']}/{report['master_symbols']} symbols, "
            f"{report['imported_rows']} rows; source through {report['source_last_update'] or 'unknown'}."
        )
        return True
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as error:
        print(f"EOD2 OHLCV bootstrap failed: {error}")
        return False


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
