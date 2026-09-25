"""Build a current, reproducible relative-strength percentile snapshot."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pandas as pd

from pipeline_utils import BASE_DIR, save_json


HORIZONS = {"one_month": 21, "three_month": 63, "twelve_month": 252}


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


def return_as_of(stock, benchmark, horizon):
    merged = stock[["Date", "Close"]].merge(benchmark[["Date", "Close"]], on="Date", suffixes=("_stock", "_benchmark"))
    if len(merged) <= horizon:
        return None
    start, end = merged.iloc[-1 - horizon], merged.iloc[-1]
    if start.Close_stock <= 0 or start.Close_benchmark <= 0:
        return None
    return ((end.Close_stock / start.Close_stock) - (end.Close_benchmark / start.Close_benchmark)) * 100


def main():
    root = Path(BASE_DIR)
    benchmark_path = root / "indices_ohlcv_data" / "NIFTY.csv"
    if not benchmark_path.exists():
        print("Error: NIFTY.csv benchmark history is missing.")
        return False
    benchmark = close_history(benchmark_path)
    eligible, universe_generated_at = liquid_universe(root)
    raw = {name: {} for name in HORIZONS}
    for path in sorted((root / "ohlcv_data").glob("*.csv")):
        if path.stem.upper() not in eligible:
            continue
        stock = close_history(path)
        for name, horizon in HORIZONS.items():
            value = return_as_of(stock, benchmark, horizon)
            if value is not None:
                raw[name][path.stem] = value
    ratings = {}
    for name, values in raw.items():
        ranked = pd.Series(values, dtype=float).rank(pct=True, method="average") * 99
        for symbol, rating in ranked.items():
            ratings.setdefault(symbol, {})[name] = round(float(rating), 4)
    as_of = benchmark["Date"].iloc[-1].date().isoformat()
    save_json(root / "rs_rating_daily.json", {
        "source": "locally computed relative-strength percentile against NIFTY",
        "as_of_date": as_of,
        "universe": "breadth_eligible",
        "universe_generated_at": universe_generated_at,
        "liquid_universe_count": len(ratings),
        "ratings": ratings,
    }, ensure_ascii=False)
    print(f"Saved RS ratings for {len(ratings)} symbols as of {as_of}.")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
