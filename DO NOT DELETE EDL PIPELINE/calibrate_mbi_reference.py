"""Calibrate reconstructed MBI calculations against published reference rows.

This is an evidence tool, not part of the production pipeline. It fetches a
short, reproducible OHLCV snapshot, evaluates plausible universe and return
definitions, and writes the ranked results to ``calibration/``.
"""

from __future__ import annotations

import json
import math
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import csv
import io
from pathlib import Path

import numpy as np
import requests

from pipeline_utils import fetch_scanx_data, get_headers, save_json


BASE_DIR = Path(__file__).resolve().parent
CACHE_PATH = BASE_DIR / "calibration" / "mbi_july_2026_snapshot.json"
REPORT_PATH = BASE_DIR / "calibration" / "mbi_calibration_report.json"
NSE_CACHE_PATH = BASE_DIR / "calibration" / "nse_bhavcopy_returns.json"
TICK_API_URL = "https://openweb-ticks.dhan.co/getDataH"

REFERENCE_45R = {
    "2026-07-15": 230.1,
    "2026-07-16": 146.9,
    "2026-07-17": 49.8,
    "2026-07-20": 232.9,
    "2026-07-21": 314.0,
    "2026-07-22": 75.5,
    "2026-07-23": 120.7,
    "2026-07-24": 182.4,
    "2026-07-27": 358.6,
    "2026-07-28": 58.7,
}

REFERENCE_XP = {
    "2026-07-15": 10.0,
    "2026-07-16": 10.0,
    "2026-07-17": 9.0,
    "2026-07-20": 9.0,
    "2026-07-21": 9.0,
    "2026-07-22": 8.0,
    "2026-07-23": 7.0,
    "2026-07-24": 6.0,
    "2026-07-27": 7.0,
    "2026-07-28": 7.0,
}

REFERENCE_EM = {
    "2026-07-15": 10.0,
    "2026-07-16": 10.0,
    "2026-07-17": 9.0,
    "2026-07-20": 10.0,
    "2026-07-21": 11.0,
    "2026-07-22": 8.0,
    "2026-07-23": 7.0,
    "2026-07-24": 7.0,
    "2026-07-27": 9.0,
    "2026-07-28": 7.0,
}

REFERENCE_EXTREME_PCTS = {
    "2026-07-15": {"advance": 3.3, "decline": 1.4},
    "2026-07-16": {"advance": 2.6, "decline": 1.8},
    "2026-07-17": {"advance": 1.4, "decline": 2.9},
    "2026-07-20": {"advance": 3.8, "decline": 1.6},
    "2026-07-21": {"advance": 4.1, "decline": 1.3},
    "2026-07-22": {"advance": 2.2, "decline": 2.9},
    "2026-07-23": {"advance": 2.5, "decline": 2.1},
    "2026-07-24": {"advance": 3.0, "decline": 1.7},
    "2026-07-27": {"advance": 5.8, "decline": 1.6},
    "2026-07-28": {"advance": 2.3, "decline": 3.9},
}

REFERENCE_MA_PCTS = {
    "2026-07-15": {10: 43.6, 20: 46.4, 50: 56.1, 200: 44.7},
    "2026-07-16": {10: 42.8, 20: 44.1, 50: 55.2, 200: 44.3},
    "2026-07-17": {10: 39.6, 20: 42.2, 50: 53.5, 200: 44.1},
    "2026-07-20": {10: 46.3, 20: 45.8, 50: 55.5, 200: 45.0},
    "2026-07-21": {10: 51.2, 20: 49.4, 50: 55.9, 200: 46.2},
    "2026-07-22": {10: 33.7, 20: 38.6, 50: 51.6, 200: 43.4},
    "2026-07-23": {10: 27.0, 20: 31.2, 50: 47.6, 200: 42.2},
    "2026-07-24": {10: 28.5, 20: 31.1, 50: 47.8, 200: 41.7},
    "2026-07-27": {10: 42.2, 20: 40.2, 50: 51.5, 200: 43.4},
    "2026-07-28": {10: 36.1, 20: 34.5, 50: 47.8, 200: 42.0},
}

REFERENCE_DERIVED = {
    "2026-07-15": {"change_4_5": 104.5, "ratio_20": 86.4, "change_20": 11.5, "ratio_50": 127.7, "change_50": 8.5, "high_52w_pct": 2.6, "low_52w_pct": 0.7, "index_change_pct": 0.52},
    "2026-07-16": {"change_4_5": -36.2, "ratio_20": 78.9, "change_20": -8.7, "ratio_50": 123.3, "change_50": -3.4, "high_52w_pct": 2.2, "low_52w_pct": 0.6, "index_change_pct": -0.24},
    "2026-07-17": {"change_4_5": -66.1, "ratio_20": 73.0, "change_20": -7.5, "ratio_50": 114.9, "change_50": -6.8, "high_52w_pct": 0.9, "low_52w_pct": 1.0, "index_change_pct": -0.46},
    "2026-07-20": {"change_4_5": 367.5, "ratio_20": 84.4, "change_20": 15.6, "ratio_50": 124.6, "change_50": 8.4, "high_52w_pct": 1.8, "low_52w_pct": 1.1, "index_change_pct": 0.38},
    "2026-07-21": {"change_4_5": 34.8, "ratio_20": 97.7, "change_20": 15.7, "ratio_50": 126.9, "change_50": 1.8, "high_52w_pct": 2.2, "low_52w_pct": 0.9, "index_change_pct": 0.39},
    "2026-07-22": {"change_4_5": -75.9, "ratio_20": 62.9, "change_20": -35.6, "ratio_50": 106.4, "change_50": -16.1, "high_52w_pct": 1.9, "low_52w_pct": 1.0, "index_change_pct": -1.14},
    "2026-07-23": {"change_4_5": 59.8, "ratio_20": 45.3, "change_20": -27.9, "ratio_50": 90.8, "change_50": -14.7, "high_52w_pct": 1.3, "low_52w_pct": 1.0, "index_change_pct": -1.04},
    "2026-07-24": {"change_4_5": 51.2, "ratio_20": 45.1, "change_20": -0.7, "ratio_50": 91.4, "change_50": 0.7, "high_52w_pct": 1.3, "low_52w_pct": 1.6, "index_change_pct": -0.15},
    "2026-07-27": {"change_4_5": 96.6, "ratio_20": 67.3, "change_20": 49.4, "ratio_50": 106.2, "change_50": 16.1, "high_52w_pct": 2.4, "low_52w_pct": 0.9, "index_change_pct": 1.15},
    "2026-07-28": {"change_4_5": -83.6, "ratio_20": 52.7, "change_20": -21.7, "ratio_50": 91.5, "change_50": -13.8, "high_52w_pct": 1.8, "low_52w_pct": 0.7, "index_change_pct": -0.16},
}


def _universe_payload():
    return {
        "data": {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 5000,
            "fields": ["Sym", "DispSym", "Sid", "Mcap", "Ltp", "Exch", "OgInst"],
            "params": [
                {"field": "OgInst", "op": "", "val": "ES"},
                {"field": "Exch", "op": "", "val": "NSE"},
            ],
            "pgno": 0,
        }
    }


def _history_payload(stock):
    return {
        "EXCH": stock.get("Exch") or "NSE",
        "SYM": stock["Sym"],
        "SEG": "E",
        "INST": "EQUITY",
        "SEC_ID": stock["Sid"],
        "EXPCODE": 0,
        "INTERVAL": "D",
        "START": int(datetime(2025, 7, 1).timestamp()),
        "END": int(datetime(2026, 7, 29).timestamp()),
    }


def _fetch_history(stock):
    for attempt in range(3):
        try:
            response = requests.post(
                TICK_API_URL,
                json=_history_payload(stock),
                headers=get_headers(include_origin=True),
                timeout=20,
            )
            response.raise_for_status()
            data = response.json().get("data", {})
            dates = data.get("Time", [])
            closes = data.get("c", [])
            if not dates or len(dates) != len(closes):
                return stock["Sym"], {}
            return stock["Sym"], {
                (
                    timestamp[:10]
                    if isinstance(timestamp, str)
                    else datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                ): float(close)
                for timestamp, close in zip(dates, closes)
                if close is not None and float(close) > 0
            }
        except (requests.RequestException, ValueError, TypeError):
            if attempt == 2:
                return stock["Sym"], {}
            time.sleep(0.25 * (2**attempt))
    return stock["Sym"], {}


def build_snapshot():
    stocks = [
        row
        for row in fetch_scanx_data(_universe_payload(), timeout=30)
        if row.get("Sym")
        and row.get("Sid")
        and row.get("Mcap") is not None
        and row.get("Ltp") is not None
    ]
    histories = {}
    with ThreadPoolExecutor(max_workers=48) as executor:
        futures = {executor.submit(_fetch_history, stock): stock["Sym"] for stock in stocks}
        for future in as_completed(futures):
            symbol, history = future.result()
            if history:
                histories[symbol] = history

    today = datetime.now().strftime("%Y-%m-%d")
    snapshot_stocks = []
    for stock in stocks:
        if stock["Sym"] not in histories:
            continue
        history = dict(histories[stock["Sym"]])
        # The daily history endpoint commonly publishes the final bar on the
        # following session. After market close, ScanX LTP is the official
        # current close and is needed for same-day EOD calibration.
        history.setdefault(today, float(stock["Ltp"]))
        snapshot_stocks.append({
            "symbol": stock["Sym"],
            "sid": stock["Sid"],
            "market_cap": float(stock["Mcap"]),
            "latest_price": float(stock["Ltp"]),
            "history": history,
        })
    snapshot = {
        "fetched_at": datetime.now().astimezone().isoformat(),
        "stocks": snapshot_stocks,
    }
    save_json(CACHE_PATH, snapshot, indent=2)
    return snapshot


def load_snapshot(refresh=False):
    if CACHE_PATH.exists() and not refresh:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return build_snapshot()


def observations(snapshot):
    rows = []
    target_dates = sorted(set(REFERENCE_45R) | set(REFERENCE_XP))
    for stock in snapshot["stocks"]:
        history = stock["history"]
        all_dates = sorted(history)
        date_index = {date: index for index, date in enumerate(all_dates)}
        for date in target_dates:
            index = date_index.get(date)
            if index is None or index == 0:
                continue
            close = float(history[date])
            previous_close = float(history[all_dates[index - 1]])
            rows.append(
                (
                    date,
                    float(stock["market_cap"]),
                    float(stock["latest_price"]),
                    close,
                    previous_close,
                    100.0 * (close / previous_close - 1.0),
                )
            )
    dtype = [
        ("date", "U10"),
        ("market_cap", "f8"),
        ("latest_price", "f8"),
        ("close", "f8"),
        ("previous_close", "f8"),
        ("return_pct", "f8"),
    ]
    return np.array(rows, dtype=dtype)


def _score_45r(calculated):
    errors = []
    for date, target in REFERENCE_45R.items():
        value = calculated.get(date)
        if value is None or value <= 0:
            return float("inf")
        errors.append((math.log(value) - math.log(target)) ** 2)
    return math.sqrt(sum(errors) / len(errors))


def calculate_ratios(
    data,
    cap_floor,
    return_threshold,
    cap_mode,
    price_mode,
    price_floor=1.0,
):
    calculated = {}
    counts = {}
    extreme_percentages = {}
    for date in sorted(set(REFERENCE_45R) | set(REFERENCE_EXTREME_PCTS)):
        day = data[data["date"] == date]
        if cap_mode == "historical_scaled":
            market_cap = day["market_cap"] * day["close"] / day["latest_price"]
        else:
            market_cap = day["market_cap"]
        price = day["close"] if price_mode == "historical_close" else day["latest_price"]
        eligible = (market_cap > cap_floor) & (price >= price_floor)
        returns = day["return_pct"][eligible]
        advances = int(np.count_nonzero(returns > return_threshold))
        declines = int(np.count_nonzero(returns < -return_threshold))
        calculated[date] = 100.0 * advances / declines if declines else None
        eligible_count = int(np.count_nonzero(eligible))
        extreme_percentages[date] = {
            "advance": 100.0 * advances / eligible_count if eligible_count else None,
            "decline": 100.0 * declines / eligible_count if eligible_count else None,
        }
        counts[date] = {
            "eligible": eligible_count,
            "advances": advances,
            "declines": declines,
        }
    return calculated, counts, extreme_percentages


def _score_extreme_percentages(calculated):
    errors = []
    for date, targets in REFERENCE_EXTREME_PCTS.items():
        values = calculated.get(date, {})
        for key, target in targets.items():
            value = values.get(key)
            if value is None:
                return float("inf")
            errors.append((value - target) ** 2)
    return math.sqrt(sum(errors) / len(errors))


def calibrate_45r(data):
    results = []
    broad_cap_floors = [
        0, 100, 250, 500, 750, 999, 1250, 1500, 2000, 2500,
        3000, 4000, 5000, 7500, 10000,
    ]
    for cap_mode in ("latest_snapshot", "historical_scaled"):
        for price_mode in ("latest_snapshot", "historical_close"):
            for cap_floor in broad_cap_floors:
                for return_threshold in np.arange(3.5, 5.51, 0.1):
                    calculated, counts, extreme_percentages = calculate_ratios(
                        data,
                        float(cap_floor),
                        float(return_threshold),
                        cap_mode,
                        price_mode,
                    )
                    ratio_score = _score_45r(calculated)
                    percentage_score = _score_extreme_percentages(extreme_percentages)
                    results.append(
                        {
                            "score": ratio_score + percentage_score / 10.0,
                            "score_log_rmse": ratio_score,
                            "score_extreme_pct_rmse": percentage_score,
                            "cap_mode": cap_mode,
                            "price_mode": price_mode,
                            "market_cap_floor_crore": round(float(cap_floor), 2),
                            "return_threshold_pct": round(float(return_threshold), 3),
                            "calculated": {
                                date: round(value, 3) if value is not None else None
                                for date, value in calculated.items()
                            },
                            "extreme_percentages": {
                                date: {
                                    key: round(value, 3) if value is not None else None
                                    for key, value in values.items()
                                }
                                for date, values in extreme_percentages.items()
                            },
                            "counts": counts,
                        }
                    )
    return sorted(results, key=lambda result: result["score"])


def calculate_ma_breadth(
    snapshot,
    cap_floor=999.0,
    price_floor=1.0,
    allowed_symbols=None,
):
    dates = sorted(REFERENCE_MA_PCTS)
    periods = (10, 20, 50, 200)
    counts = {
        date: {
            ma_type: {
                period: {"eligible": 0, "valid": 0, "above": 0}
                for period in periods
            }
            for ma_type in ("sma", "ema")
        }
        for date in dates
    }
    for stock in snapshot["stocks"]:
        if allowed_symbols is not None and stock["symbol"] not in allowed_symbols:
            continue
        if stock["market_cap"] <= cap_floor or stock["latest_price"] < price_floor:
            continue
        history = stock["history"]
        history_dates = sorted(history)
        closes = [float(history[date]) for date in history_dates]
        date_index = {date: index for index, date in enumerate(history_dates)}
        ema_values = {}
        for period in periods:
            alpha = 2.0 / (period + 1.0)
            values = []
            value = closes[0]
            for close in closes:
                value = alpha * close + (1.0 - alpha) * value
                values.append(value)
            ema_values[period] = values
        for date in dates:
            index = date_index.get(date)
            if index is None:
                continue
            for period in periods:
                for ma_type in ("sma", "ema"):
                    cell = counts[date][ma_type][period]
                    cell["eligible"] += 1
                    if index + 1 < period:
                        continue
                    moving_average = (
                        sum(closes[index + 1 - period:index + 1]) / period
                        if ma_type == "sma"
                        else ema_values[period][index]
                    )
                    cell["valid"] += 1
                    cell["above"] += int(closes[index] > moving_average)

    output = {}
    for date, type_counts in counts.items():
        output[date] = {}
        for ma_type, period_counts in type_counts.items():
            output[date][ma_type] = {}
            for period, cell in period_counts.items():
                output[date][ma_type][period] = {
                    **cell,
                    "pct_valid_denominator": (
                        100.0 * cell["above"] / cell["valid"] if cell["valid"] else None
                    ),
                    "pct_eligible_denominator": (
                        100.0 * cell["above"] / cell["eligible"] if cell["eligible"] else None
                    ),
                }
    return output


def calculate_xp_series(
    snapshot,
    cap_floor=100.0,
    price_floor=1.0,
    start_date=None,
    allowed_symbols=None,
):
    aggregates = defaultdict(
        lambda: {
            "eligible": 0,
            "up_4_5": 0,
            "down_4_5": 0,
            "above_10": 0,
            "above_20": 0,
        }
    )
    for stock in snapshot["stocks"]:
        if allowed_symbols is not None and stock["symbol"] not in allowed_symbols:
            continue
        if stock["market_cap"] <= cap_floor or stock["latest_price"] < price_floor:
            continue
        history_dates = sorted(stock["history"])
        closes = [float(stock["history"][date]) for date in history_dates]
        for index, date in enumerate(history_dates):
            row = aggregates[date]
            row["eligible"] += 1
            if index:
                daily_return = 100.0 * (closes[index] / closes[index - 1] - 1.0)
                row["up_4_5"] += int(daily_return >= 4.5)
                row["down_4_5"] += int(daily_return < -4.5)
            if index + 1 >= 10:
                row["above_10"] += int(
                    closes[index] > sum(closes[index - 9:index + 1]) / 10.0
                )
            if index + 1 >= 20:
                row["above_20"] += int(
                    closes[index] > sum(closes[index - 19:index + 1]) / 20.0
                )

    previous_xp = 12.0
    previous_z = None
    output = {}
    for date in sorted(aggregates):
        if start_date is not None and date < start_date:
            continue
        row = aggregates[date]
        eligible = row["eligible"]
        if not eligible:
            continue
        up_count = row["up_4_5"]
        down_count = row["down_4_5"]
        up_pct = 100.0 * up_count / eligible
        down_pct = 100.0 * down_count / eligible
        p10 = 100.0 * row["above_10"] / eligible
        p20 = 100.0 * row["above_20"] / eligible
        z = up_count if previous_z is None else 0.162 * up_count + 0.838 * previous_z
        previous_z = z
        bounded_p10 = min(max(p10, 0.01), 99.99)
        bounded_p20 = min(max(p20, 0.01), 99.99)
        log_xp = (
            0.592 * math.log(previous_xp)
            + 0.471 * math.log(max(z, 0.5))
            + 0.198 * math.log(bounded_p10 / (100.0 - bounded_p10))
            + 0.334
            - 0.067 * math.log(max(down_count, 0.5))
            - 0.077 * math.log(bounded_p20 / (100.0 - bounded_p20))
        )
        previous_xp = math.exp(log_xp)
        output[date] = {
            **row,
            "up_4_5_pct": up_pct,
            "down_4_5_pct": down_pct,
            "xp_advancer_count": up_count,
            "xp_decliner_count": down_count,
            "above_10_pct": p10,
            "above_20_pct": p20,
            "xp": previous_xp,
            "z": z,
        }
    return {date: output.get(date) for date in REFERENCE_XP}


def evaluate_xp_output_calibration(series):
    """Score a transparent output-only calibration against displayed XP."""
    pairs = [
        (float(series[date]["xp"]), float(target))
        for date, target in REFERENCE_XP.items()
        if series.get(date) is not None
    ]
    least_squares_multiplier = (
        sum(raw * target for raw, target in pairs)
        / sum(raw * raw for raw, _ in pairs)
    )
    rounding_lower = max((target - 0.5) / raw for raw, target in pairs)
    rounding_upper = min((target + 0.5) / raw for raw, target in pairs)
    has_exact_rounding_interval = rounding_lower < rounding_upper
    constrained = min(least_squares_multiplier, rounding_upper)
    selected_multiplier = (
        math.floor(constrained * 100000.0) / 100000.0
        if has_exact_rounding_interval
        else least_squares_multiplier
    )

    def metrics(multiplier):
        errors = [raw * multiplier - target for raw, target in pairs]
        return {
            "multiplier": multiplier,
            "mae": sum(abs(error) for error in errors) / len(errors),
            "rmse": math.sqrt(
                sum(error * error for error in errors) / len(errors)
            ),
            "rounded_matches": sum(
                round(raw * multiplier) == target for raw, target in pairs
            ),
            "observations": len(errors),
            "calculated": {
                date: series[date]["xp"] * multiplier
                for date in REFERENCE_XP
                if series.get(date) is not None
            },
        }

    return {
        "raw": metrics(1.0),
        "least_squares": metrics(least_squares_multiplier),
        "exact_rounding_interval": (
            [rounding_lower, rounding_upper]
            if has_exact_rounding_interval
            else None
        ),
        "selected": metrics(selected_multiplier),
        "warning": (
            "Output-only calibration on ten published dates. Revalidate on "
            "unseen dates and whenever the upstream universe changes."
        ),
    }


def evaluate_em_availability():
    differences = [
        REFERENCE_EM[date] - REFERENCE_XP[date]
        for date in REFERENCE_EM
    ]
    return {
        "status": "not_reconstructable_from_current_public_inputs",
        "observations": len(differences),
        "xp_as_em_baseline_mae": (
            sum(abs(value) for value in differences) / len(differences)
        ),
        "xp_as_em_exact_matches": sum(value == 0 for value in differences),
        "difference_sequence": {
            date: REFERENCE_EM[date] - REFERENCE_XP[date]
            for date in REFERENCE_EM
        },
        "reason": (
            "The indicator identifies EM as a Pine Seed score but does not "
            "publish its formula or raw seed series. Ten integer observations "
            "are insufficient to validate an independent model."
        ),
    }


def load_nse_bhavcopy_returns(refresh=False):
    if NSE_CACHE_PATH.exists() and not refresh:
        return json.loads(NSE_CACHE_PATH.read_text(encoding="utf-8"))
    output = {}
    for date in sorted(REFERENCE_45R):
        formatted = datetime.strptime(date, "%Y-%m-%d").strftime("%d%m%Y")
        url = (
            "https://nsearchives.nseindia.com/products/content/"
            f"sec_bhavdata_full_{formatted}.csv"
        )
        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"},
            timeout=30,
        )
        response.raise_for_status()
        rows = []
        for raw in csv.DictReader(io.StringIO(response.text)):
            row = {
                str(key).strip(): value.strip() if isinstance(value, str) else value
                for key, value in raw.items()
            }
            try:
                previous_close = float(row["PREV_CLOSE"])
                close = float(row["CLOSE_PRICE"])
            except (KeyError, TypeError, ValueError):
                continue
            if previous_close <= 0 or close <= 0:
                continue
            rows.append({
                "symbol": row.get("SYMBOL"),
                "series": row.get("SERIES"),
                "previous_close": previous_close,
                "close": close,
                "return_pct": 100.0 * (close / previous_close - 1.0),
            })
        output[date] = rows
    save_json(NSE_CACHE_PATH, output, indent=2)
    return output


def calculate_nse_ratios(
    snapshot,
    bhavcopy,
    series,
    cap_floor=100.0,
    price_floor=1.0,
):
    universe = {
        stock["symbol"]: stock
        for stock in snapshot["stocks"]
        if stock["market_cap"] > cap_floor and stock["latest_price"] >= price_floor
    }
    ratios = {}
    percentages = {}
    counts = {}
    for date, rows in bhavcopy.items():
        selected = [
            row
            for row in rows
            if row["symbol"] in universe and (series is None or row["series"] in series)
        ]
        advances = sum(row["return_pct"] >= 4.5 for row in selected)
        declines = sum(row["return_pct"] < -4.5 for row in selected)
        eligible = len(selected)
        ratios[date] = 100.0 * advances / declines if declines else None
        percentages[date] = {
            "advance": 100.0 * advances / eligible if eligible else None,
            "decline": 100.0 * declines / eligible if eligible else None,
        }
        counts[date] = {
            "eligible": eligible,
            "advances": advances,
            "declines": declines,
        }
    return {
        "score_log_rmse": _score_45r(ratios),
        "score_extreme_pct_rmse": _score_extreme_percentages(percentages),
        "ratios": ratios,
        "percentages": percentages,
        "counts": counts,
    }


def main():
    refresh = "--refresh" in __import__("sys").argv
    snapshot = load_snapshot(refresh=refresh)
    nse_bhavcopy = load_nse_bhavcopy_returns(refresh=refresh)
    calibrated_series = {"EQ", "BE", "BZ", "ST"}
    latest_bhavcopy_date = max(nse_bhavcopy)
    calibrated_symbols = {
        row["symbol"]
        for row in nse_bhavcopy[latest_bhavcopy_date]
        if row["series"] in calibrated_series
    }
    data = observations(snapshot)
    ranked = calibrate_45r(data)
    xp_cap_100 = calculate_xp_series(snapshot)
    report = {
        "reference_4_5r": REFERENCE_45R,
        "reference_xp": REFERENCE_XP,
        "reference_em": REFERENCE_EM,
        "reference_extreme_percentages": REFERENCE_EXTREME_PCTS,
        "reference_ma_percentages": REFERENCE_MA_PCTS,
        "reference_derived": REFERENCE_DERIVED,
        "ma_breadth_cap_100": calculate_ma_breadth(snapshot, cap_floor=100.0),
        "ma_breadth_cap_100_calibrated_series": calculate_ma_breadth(
            snapshot,
            cap_floor=100.0,
            allowed_symbols=calibrated_symbols,
        ),
        "ma_breadth_cap_999": calculate_ma_breadth(snapshot),
        "xp_series_cap_100": xp_cap_100,
        "xp_output_calibration_cap_100": evaluate_xp_output_calibration(
            xp_cap_100
        ),
        "em_availability": evaluate_em_availability(),
        "xp_series_cap_100_ytd": calculate_xp_series(
            snapshot,
            start_date="2026-01-01",
        ),
        "xp_series_cap_100_calibrated_series": calculate_xp_series(
            snapshot,
            allowed_symbols=calibrated_symbols,
        ),
        "nse_bhavcopy_eq": calculate_nse_ratios(snapshot, nse_bhavcopy, {"EQ"}),
        "nse_bhavcopy_calibrated_series": calculate_nse_ratios(
            snapshot,
            nse_bhavcopy,
            calibrated_series,
        ),
        "nse_bhavcopy_all_series": calculate_nse_ratios(snapshot, nse_bhavcopy, None),
        "snapshot_fetched_at": snapshot["fetched_at"],
        "stock_histories": len(snapshot["stocks"]),
        "top_4_5r_candidates": ranked[:50],
    }
    save_json(REPORT_PATH, report, indent=2)
    print(json.dumps(report["top_4_5r_candidates"][:5], indent=2))
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
