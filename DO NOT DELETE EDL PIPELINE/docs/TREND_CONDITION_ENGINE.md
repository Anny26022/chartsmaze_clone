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
- `delivery_percent_spike`
- `new_high` and `new_low`
- `percent_from_52w_high` and `percent_from_52w_low`
- `consolidation_range`
- `atr_percent`
- `range_contraction`
- `inside_bar`
- `unfilled_gap`
- `vcp_contraction_legs`
- `horizontal_resistance_line`

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
- `delivery_percent_spike` reads the cached `delivery_history_data/` directory.
  The full pipeline maintains the newest 260 published NSE sessions there;
  its first run downloads the one official full-universe bhavcopy for each
  missing calendar date, and later runs fetch only missing/new files.
- `new_high` and `new_low` compare a session's high/low with its completed
  rolling lookback. Their `fired_within` behaves identically to other recent
  signal controls: one means the latest session only.
- The 52-week distance rules use 252 sessions and the session high/low, not
  the close-only return. A high-distance of 5 means the latest close is 5%
  below the 252-session highest high; a low-distance of 5 means it is 5%
  above the 252-session lowest low.
- `consolidation_range` is `(max(high) - min(low)) / final_close * 100` over
  the base. `exclude_latest` removes that many latest sessions before the base
  is selected, allowing a breakout session to be tested outside its base.
- `atr_percent` is Wilder ATR (`alpha = 1 / period`) divided by latest close.
- `range_contraction` compares the literal high-to-low widths of a recent and
  base window. `prior_mode: "prior"` uses the immediately preceding, disjoint base;
  `nested` uses an enclosing base and is normally the weaker test.
- `inside_bar` evaluates the latest requested run of daily bars, or ISO-week
  OHLCV bars for `timeframe: "weekly"`. As on the referenced screener, the
  current partial week participates while it is in progress.
- `unfilled_gap` defines a gap relative to the prior close. An up-gap fills
  when a later low reaches that prior close; a down-gap fills when a later high
  reaches it. `state` selects `unfilled` or `filled` events.
- `vcp_contraction_legs` uses a deterministic close-reversal zig-zag to find
  high/low pivots, then tests the latest alternating swing legs for progressively
  smaller percentage moves. `minimum_swing_percent` is the reversal/noise floor.
- `horizontal_resistance_line` starts with the newest swing-high pivot not
  exceeded by any later **close** (wicks do not break it). Older unbroken swing
  highs within the ceiling tolerance are clustered into the same line. The
  base age/depth and price-to-line/20-EMA constraints are then applied.

### JournalToday field-name compatibility

The machine-readable registry uses the pipeline's snake_case names. The
evaluator also accepts the corresponding visible JournalToday keys where they
differ, such as `lookbackDays`, `withinDays`, `maxRangePct`, `recentDays`,
`priorMode`, `minGapPct`, `minLegs`, and `clusterTolerancePct`. This permits a
saved UI rule to be translated without silently changing its parameters.

The generated output includes each condition's result and details. By default
only matches are emitted; use `include_non_matches` in the request or
`--include-non-matches` for diagnostics.
