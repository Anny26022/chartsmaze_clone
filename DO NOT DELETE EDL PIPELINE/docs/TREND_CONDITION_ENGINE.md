# Daily trend-condition engine

`screen_trend_conditions.py` evaluates an ANDed JSON request against the local
daily `ohlcv_data/*.csv` cache. It is deliberately local and reproducible: it
does not make a network request while screening.

```bash
python3 screen_trend_conditions.py \
  --request examples/trend-screen-request.json \
  --output output/trend-screen.json
```

Use `--list-conditions` to obtain the machine-readable registry of supported
controls. Supported daily-OHLCV conditions are:

- `persistent_momentum`
- `price_vs_ema`
- `ema_shakeout_reclaim`
- `adx`
- `price_vs_sma`
- `percent_days_above_ma`
- `ma_stack`
- `ma_slope`
- `price_change_percent`
- `consecutive_up_days`
- `gap_up` and `gap_down`
- `relative_volume`
- `volume_trend`
- `highest_volume`

## Calculation contract

- Conditions are evaluated on daily OHLCV through `as_of_date`; if omitted,
  the most recent session in each symbol's cache is used.
- Multiple conditions are ANDed. `persistent_momentum` is the one exception:
  it matches if *any* selected EMA period satisfies its own required run.
- A missing required moving-average or ADX warm-up returns `unavailable`, not
  `match` or `no_match`.
- EMA uses `adjust=False`; ADX uses Wilder smoothing (`alpha = 1 / period`).
- `strict_close` persistence requires every close to remain on the selected
  side. `reclaim_by_extreme` permits exactly one contrary close when a later
  bar crosses that breach bar's high for an above-run (or low for a below-run)
  and closes back on the desired side. This makes the previously ambiguous
  reclaim behavior explicit and testable; it is not claimed to be proprietary
  JournalToday parity.
- `ema_shakeout_reclaim` requires a current close above the EMA plus a low-side
  or close-side dip in the requested recent window.
- `fired_within` is counted in trading sessions, including the latest session:
  `1` means the signal must be present today; `2` also accepts yesterday.
- Relative volume compares a day's volume with the *preceding* `average_window`
  sessions, avoiding look-ahead bias. Volume trend compares the most recent
  window with the immediately preceding base window.
- Delivery % is deliberately not a condition in this OHLCV-only engine. The
  EDL now publishes the latest delivery snapshot separately; a historical
  delivery-series artifact is still required for a faithful `fired_within`
  delivery-spike rule.

The generated output includes each condition's result and details. By default
only matches are emitted; use `include_non_matches` in the request or
`--include-non-matches` for diagnostics.
