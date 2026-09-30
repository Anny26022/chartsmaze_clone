"""Build current, reproducible cross-sectional relative-strength ratings."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pandas as pd

from pipeline_utils import BASE_DIR, save_json


FRONT_WEIGHTED_HORIZONS = {
    "one_month": 21,
    "three_month": 63,
    "six_month": 126,
    "twelve_month": 252,
}
FRONT_WEIGHTED_WEIGHTS = {
    "one_month": 0.40,
    "three_month": 0.20,
    "six_month": 0.20,
    "twelve_month": 0.20,
}
MINIMUM_ALIGNED_SESSIONS = 260
FRONT_WEIGHTED_BENCHMARK_FILENAME = "NIFTY_500.csv"


def liquid_universe(root):
    """Use the pipeline's documented breadth-eligible universe for RS ranks."""
    path = root / "breadth_universe_snapshot.json"
    if path.exists():
        opener = open
    else:
        path = path.with_suffix(".json.gz")
        opener = gzip.open
    if not path.exists():
        raise FileNotFoundError("breadth_universe_snapshot.json(.gz) is required before RS ratings")
    with opener(path, "rt", encoding="utf-8") as handle:
        snapshot = json.load(handle)
    symbols = {str(item["symbol"]).upper() for item in snapshot.get("eligible", []) if item.get("symbol")}
    if not symbols:
        raise ValueError("breadth universe contains no eligible symbols")
    return symbols, snapshot.get("generated_at")


def close_history(path):
    frame = pd.read_csv(path)
    frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
    frame["Close"] = pd.to_numeric(frame["Close"], errors="coerce")
    return frame.dropna(subset=("Date", "Close")).sort_values("Date").drop_duplicates("Date", keep="last")


def relative_strength_scores(stock, benchmark, as_of_date):
    """Return Nifty 500-relative one-month and front-weighted scores.

    A rating is only valid when all four horizon endpoints are aligned to the
    benchmark's current completed session.  This prevents a stale stock file
    from receiving a rating labelled with a newer market date.
    """
    merged = stock[["Date", "Close"]].merge(
        benchmark[["Date", "Close"]], on="Date", suffixes=("_stock", "_benchmark")
    )
    if len(merged) < MINIMUM_ALIGNED_SESSIONS:
        return None
    if merged["Date"].iloc[-1].date() != as_of_date:
        return None

    relative_ratios = {}
    for name, horizon in FRONT_WEIGHTED_HORIZONS.items():
        start, end = merged.iloc[-1 - horizon], merged.iloc[-1]
        if min(start.Close_stock, end.Close_stock, start.Close_benchmark, end.Close_benchmark) <= 0:
            return None
        stock_ratio = end.Close_stock / start.Close_stock
        benchmark_ratio = end.Close_benchmark / start.Close_benchmark
        relative_ratios[name] = stock_ratio / benchmark_ratio

    # Compare each horizon with its matching benchmark horizon before applying
    # the weights.  Dividing one aggregate score by one market-wide constant
    # would not affect the cross-sectional rank.
    return {
        **relative_ratios,
        "front_weighted": sum(
            FRONT_WEIGHTED_WEIGHTS[name] * relative_ratios[name]
            for name in FRONT_WEIGHTED_HORIZONS
        ),
    }


def percentile_ratings(scores):
    """Map scores to the documented bounded 1–99 strict-lower percentile."""
    values = pd.Series(scores, dtype=float)
    if values.empty:
        return {}
    lower_count = values.rank(method="min") - 1
    ratings = (1 + (lower_count / len(values)) * 98).round().clip(1, 99).astype(int)
    return {symbol: int(rating) for symbol, rating in ratings.items()}


def main():
    root = Path(BASE_DIR)
    front_weighted_benchmark_path = root / "indices_ohlcv_data" / FRONT_WEIGHTED_BENCHMARK_FILENAME
    if not front_weighted_benchmark_path.exists():
        print(f"Error: {FRONT_WEIGHTED_BENCHMARK_FILENAME} benchmark history is missing.")
        return False
    front_weighted_benchmark = close_history(front_weighted_benchmark_path)
    if front_weighted_benchmark.empty:
        print(f"Error: {FRONT_WEIGHTED_BENCHMARK_FILENAME} contains no valid closes.")
        return False
    as_of_date = front_weighted_benchmark["Date"].iloc[-1].date()
    eligible, universe_generated_at = liquid_universe(root)
    scores = {rating_key: {} for rating_key in (*FRONT_WEIGHTED_HORIZONS, "front_weighted")}
    for path in sorted((root / "ohlcv_data").glob("*.csv")):
        if path.stem.upper() not in eligible:
            continue
        stock = close_history(path)
        relative_scores = relative_strength_scores(stock, front_weighted_benchmark, as_of_date)
        if relative_scores is not None:
            for rating_key, score in relative_scores.items():
                scores[rating_key][path.stem] = score

    ranked = {rating_key: percentile_ratings(values) for rating_key, values in scores.items()}
    ratings = {
        symbol: {rating_key: values[symbol] for rating_key, values in ranked.items()}
        for symbol in ranked["front_weighted"]
    }
    if not ratings:
        print("Error: no breadth-eligible symbols have 260 aligned NIFTY 500 sessions.")
        return False

    as_of = as_of_date.isoformat()
    save_json(root / "rs_rating_daily.json", {
        "source": "locally computed cross-sectional relative-strength percentiles",
        "as_of_date": as_of,
        "universe": "breadth_eligible",
        "universe_generated_at": universe_generated_at,
        "liquid_universe_count": len(ratings),
        "methodology": {
            "version": "nexus_front_weighted_v2",
            "benchmark": "NIFTY 500",
            "benchmark_file": FRONT_WEIGHTED_BENCHMARK_FILENAME,
            "windows": FRONT_WEIGHTED_HORIZONS,
            "weights": FRONT_WEIGHTED_WEIGHTS,
            "minimum_aligned_sessions": MINIMUM_ALIGNED_SESSIONS,
            "rating_keys": ["one_month", "three_month", "six_month", "twelve_month", "front_weighted"],
            "score_formula": "sum(weight_horizon * (stock_price_ratio_horizon / nifty_500_price_ratio_horizon))",
            "horizon_score_formula": "stock_horizon_price_ratio / nifty_500_horizon_price_ratio",
            "rank_formula": "1 + round(98 * strictly_lower_scores / eligible_scores)",
        },
        "ratings": ratings,
    }, ensure_ascii=False)
    print(
        f"Saved front-weighted Nifty 500 RS ratings for {len(ratings)} symbols as of {as_of}."
    )
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
