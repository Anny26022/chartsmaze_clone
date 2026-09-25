"""Daily trend-condition engine over locally cached OHLCV history.

The engine deliberately returns ``unavailable`` rather than guessing when a
symbol does not have sufficient history.  Conditions in a request are ANDed;
the ``persistent_momentum`` condition itself is an explicit any-of EMA rule.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = ("Date", "Open", "High", "Low", "Close", "Volume")
COMPARISONS = {
    "greater": lambda left, right: left > right,
    "greater_or_equal": lambda left, right: left >= right,
    "less": lambda left, right: left < right,
    "less_or_equal": lambda left, right: left <= right,
    "equal": lambda left, right: left == right,
}

# This is intentionally data, rather than UI code, so a web client can render
# the exact supported controls without duplicating the calculation contract.
CONDITION_REGISTRY = {
    "persistent_momentum": {
        "inputs": {"periods": "integer[]", "persist_days": "integer | {period: integer}"},
        "definition": "Any requested EMA period has stayed below the close for its required run.",
    },
    "price_vs_ema": {
        "inputs": {"period": "integer", "comparison": "above|below", "persist_days": "integer", "persistence_mode": "strict_close|reclaim_by_extreme"},
        "definition": "Close stays on the selected side of the EMA for a run; reclaim mode permits one reclaimed breach.",
    },
    "ema_shakeout_reclaim": {
        "inputs": {"period": "integer", "dip_within": "integer", "dip_basis": "low|close"},
        "definition": "A recent dip below the EMA followed by a latest close back above it.",
    },
    "adx": {
        "inputs": {"period": "integer", "comparison": "comparison", "value": "number"},
        "definition": "Wilder ADX, a direction-neutral trend-strength measure.",
    },
    "price_vs_sma": {
        "inputs": {"period": "integer", "comparison": "above|below", "persist_days": "integer", "persistence_mode": "strict_close|reclaim_by_extreme"},
        "definition": "Close stays on the selected side of the SMA for a run.",
    },
    "percent_days_above_ma": {
        "inputs": {"period": "integer", "ma_type": "sma|ema", "window": "integer", "comparison": "comparison", "value": "number"},
        "definition": "Percentage of closes above the selected moving average in the window.",
    },
    "ma_stack": {
        "inputs": {"periods": "integer[]", "ma_type": "sma|ema", "price_above_fastest": "boolean"},
        "definition": "Moving averages are strictly ordered from the shortest to longest period.",
    },
    "ma_slope": {
        "inputs": {"period": "integer", "ma_type": "sma|ema", "window": "integer", "comparison": "comparison", "value": "number"},
        "definition": "Percent change in a moving average from the start to the end of a window.",
    },
}


@dataclass(frozen=True)
class ConditionResult:
    condition: str
    status: str
    value: float | bool | None
    details: dict[str, Any]

    def as_dict(self):
        return {
            "condition": self.condition,
            "status": self.status,
            "value": self.value,
            "details": self.details,
        }


def _result(condition, matched, value=None, **details):
    return ConditionResult(condition, "match" if matched else "no_match", value, details)


def _unavailable(condition, reason):
    return ConditionResult(condition, "unavailable", None, {"reason": reason})


def _comparison(value, comparison, target):
    try:
        return bool(COMPARISONS[comparison](value, target))
    except KeyError as error:
        raise ValueError(f"Unsupported comparison: {comparison}") from error


def normalize_history(rows: pd.DataFrame, as_of_date: str | None = None):
    missing = [column for column in REQUIRED_COLUMNS if column not in rows.columns]
    if missing:
        raise ValueError(f"Missing OHLCV columns: {', '.join(missing)}")
    frame = rows.loc[:, REQUIRED_COLUMNS].copy()
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    for column in REQUIRED_COLUMNS[1:]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.replace([np.inf, -np.inf], np.nan).dropna(subset=("Date", "Open", "High", "Low", "Close"))
    frame = frame.loc[(frame["Close"] > 0) & (frame["High"] >= frame["Low"])]
    if as_of_date:
        cutoff = pd.Timestamp(as_of_date)
        frame = frame.loc[frame["Date"] <= cutoff]
    return frame.sort_values("Date").drop_duplicates("Date", keep="last").reset_index(drop=True)


def _ma(frame, ma_type, period):
    period = int(period)
    if period <= 0:
        raise ValueError("Moving-average period must be positive.")
    if ma_type == "sma":
        return frame["Close"].rolling(period, min_periods=period).mean()
    if ma_type == "ema":
        return frame["Close"].ewm(span=period, adjust=False, min_periods=period).mean()
    raise ValueError("ma_type must be 'sma' or 'ema'.")


def _adx(frame, period):
    period = int(period)
    if period <= 0:
        raise ValueError("ADX period must be positive.")
    high, low, close = frame["High"], frame["Low"], frame["Close"]
    previous_close = close.shift(1)
    true_range = pd.concat((high - low, (high - previous_close).abs(), (low - previous_close).abs()), axis=1).max(axis=1)
    upward = high.diff()
    downward = -low.diff()
    plus_dm = upward.where((upward > downward) & (upward > 0), 0.0)
    minus_dm = downward.where((downward > upward) & (downward > 0), 0.0)
    atr = true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr
    denominator = (plus_di + minus_di).replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / denominator
    return dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _persisted(frame, average, comparison, days, mode):
    days = int(days)
    if days <= 0:
        raise ValueError("persist_days must be positive.")
    window = frame.tail(days)
    averages = average.tail(days)
    if len(window) < days or averages.isna().any():
        return None
    desired = window["Close"] > averages if comparison == "above" else window["Close"] < averages
    if comparison not in {"above", "below"}:
        raise ValueError("comparison must be 'above' or 'below'.")
    if mode == "strict_close":
        return bool(desired.all())
    if mode != "reclaim_by_extreme":
        raise ValueError("persistence_mode must be 'strict_close' or 'reclaim_by_extreme'.")

    # A single contrary close does not reset the run when a later bar trades
    # through that breach bar's extreme and closes back on the desired side.
    breaches = np.flatnonzero(~desired.to_numpy())
    if len(breaches) == 0:
        return True
    if len(breaches) != 1:
        return False
    breach_index = int(breaches[0])
    later = window.iloc[breach_index + 1:]
    if later.empty:
        return False
    breach = window.iloc[breach_index]
    if comparison == "above":
        reclaimed = (later["High"] >= breach["High"]) & (later["Close"] > averages.iloc[breach_index + 1:])
    else:
        reclaimed = (later["Low"] <= breach["Low"]) & (later["Close"] < averages.iloc[breach_index + 1:])
    return bool(reclaimed.any())


def _evaluate(frame, spec):
    condition = spec.get("condition") or spec.get("id")
    if condition not in CONDITION_REGISTRY:
        raise ValueError(f"Unsupported trend condition: {condition!r}")
    if frame.empty:
        return _unavailable(condition, "no_ohlcv_history")

    if condition == "persistent_momentum":
        periods = [int(period) for period in spec.get("periods", (10, 20, 50))]
        required = spec.get("persist_days", 1)
        outcomes = {}
        for period in periods:
            days = int(required.get(str(period), required.get(period, 1)) if isinstance(required, dict) else required)
            persisted = _persisted(frame, _ma(frame, "ema", period), "above", days, "strict_close")
            outcomes[str(period)] = persisted
        if any(value is None for value in outcomes.values()):
            return _unavailable(condition, "insufficient_history")
        return _result(condition, any(outcomes.values()), any(outcomes.values()), qualifying_periods=[key for key, value in outcomes.items() if value], runs=outcomes)

    if condition in {"price_vs_ema", "price_vs_sma"}:
        ma_type = "ema" if condition == "price_vs_ema" else "sma"
        persisted = _persisted(frame, _ma(frame, ma_type, spec["period"]), spec["comparison"], spec["persist_days"], spec.get("persistence_mode", "strict_close"))
        if persisted is None:
            return _unavailable(condition, "insufficient_history")
        return _result(condition, persisted, persisted, period=int(spec["period"]), comparison=spec["comparison"], persist_days=int(spec["persist_days"]), persistence_mode=spec.get("persistence_mode", "strict_close"))

    if condition == "ema_shakeout_reclaim":
        average = _ma(frame, "ema", spec["period"])
        dip_within = int(spec["dip_within"])
        recent = frame.tail(dip_within)
        recent_average = average.tail(dip_within)
        if len(recent) < dip_within or recent_average.isna().any():
            return _unavailable(condition, "insufficient_history")
        dip_basis = spec.get("dip_basis", "low")
        dips = recent["Low"] < recent_average if dip_basis == "low" else recent["Close"] < recent_average
        if dip_basis not in {"low", "close"}:
            raise ValueError("dip_basis must be 'low' or 'close'.")
        matched = bool(dips.any() and recent["Close"].iloc[-1] > recent_average.iloc[-1])
        return _result(condition, matched, matched, period=int(spec["period"]), dip_within=dip_within, dip_basis=dip_basis)

    if condition == "adx":
        value = _adx(frame, spec["period"]).iloc[-1]
        if pd.isna(value):
            return _unavailable(condition, "insufficient_history")
        return _result(condition, _comparison(float(value), spec["comparison"], float(spec["value"])), round(float(value), 6), period=int(spec["period"]), comparison=spec["comparison"], target=float(spec["value"]))

    if condition == "percent_days_above_ma":
        window = int(spec["window"])
        average = _ma(frame, spec.get("ma_type", "sma"), spec["period"])
        values = frame.tail(window)
        averages = average.tail(window)
        if len(values) < window or averages.isna().any():
            return _unavailable(condition, "insufficient_history")
        percentage = float((values["Close"] > averages).mean() * 100)
        return _result(condition, _comparison(percentage, spec["comparison"], float(spec["value"])), round(percentage, 6), period=int(spec["period"]), ma_type=spec.get("ma_type", "sma"), window=window, comparison=spec["comparison"], target=float(spec["value"]))

    if condition == "ma_stack":
        periods = sorted(int(period) for period in spec["periods"])
        if len(periods) < 2 or len(set(periods)) != len(periods):
            raise ValueError("ma_stack needs at least two distinct periods.")
        averages = [_ma(frame, spec.get("ma_type", "sma"), period).iloc[-1] for period in periods]
        if any(pd.isna(value) for value in averages):
            return _unavailable(condition, "insufficient_history")
        matched = all(left > right for left, right in zip(averages, averages[1:]))
        if spec.get("price_above_fastest", False):
            matched = matched and bool(frame["Close"].iloc[-1] > averages[0])
        return _result(condition, matched, matched, periods=periods, ma_type=spec.get("ma_type", "sma"), averages=[round(float(value), 6) for value in averages])

    if condition == "ma_slope":
        window = int(spec["window"])
        average = _ma(frame, spec.get("ma_type", "sma"), spec["period"])
        if len(average) <= window or pd.isna(average.iloc[-1]) or pd.isna(average.iloc[-1 - window]) or average.iloc[-1 - window] == 0:
            return _unavailable(condition, "insufficient_history")
        slope = float((average.iloc[-1] / average.iloc[-1 - window] - 1) * 100)
        return _result(condition, _comparison(slope, spec["comparison"], float(spec["value"])), round(slope, 6), period=int(spec["period"]), ma_type=spec.get("ma_type", "sma"), window=window, comparison=spec["comparison"], target=float(spec["value"]))

    raise AssertionError("registry and evaluator are out of sync")


def evaluate_history(rows, conditions, as_of_date: str | None = None):
    """Evaluate an ANDed list of condition specs for one symbol's OHLCV rows."""
    frame = normalize_history(pd.DataFrame(rows), as_of_date)
    results = [_evaluate(frame, spec) for spec in conditions]
    statuses = [result.status for result in results]
    overall = "unavailable" if "unavailable" in statuses else ("match" if all(status == "match" for status in statuses) else "no_match")
    return {
        "status": overall,
        "as_of_date": frame["Date"].iloc[-1].strftime("%Y-%m-%d") if not frame.empty else None,
        "conditions": [result.as_dict() for result in results],
    }


def evaluate_universe(ohlcv_directory, conditions, as_of_date: str | None = None, include_non_matches=False):
    """Evaluate cached daily CSVs, returning only matches unless requested otherwise."""
    directory = Path(ohlcv_directory)
    results = []
    counts = {"match": 0, "no_match": 0, "unavailable": 0}
    for path in sorted(directory.glob("*.csv")):
        outcome = evaluate_history(pd.read_csv(path), conditions, as_of_date)
        counts[outcome["status"]] += 1
        if include_non_matches or outcome["status"] == "match":
            results.append({"symbol": path.stem, **outcome})
    return {
        "generated_at": date.today().isoformat(),
        "as_of_date": as_of_date,
        "condition_count": len(conditions),
        "counts": counts,
        "results": results,
    }
