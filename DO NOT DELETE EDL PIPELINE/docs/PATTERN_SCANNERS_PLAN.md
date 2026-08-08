# Pattern Scanner Implementation Plan

Status: deferred for later implementation.

This document records the agreed plan for reproducing the ChartMaze pattern and
event datasets inspected on 8 August 2026. The downloaded files contain results,
not their source formulas. Exact ChartMaze parity is therefore not guaranteed for
subjective patterns unless their rules become available.

## Goals

- Add deterministic daily and weekly pattern scanners using existing OHLCV data.
- Preserve signal dates and drawing metadata so results are explainable.
- Exclude SME stocks from default scanner results through
  `default_screener_eligible`, while retaining them in the master artifact.
- Keep bulk deals, block deals and circuit revisions as source events rather than
  technical-pattern calculations.
- Load each stock's OHLCV once and run all detectors in one processing stage.
- Do not add the known-unreliable RS rating and industry RS rank fields.

## Current Coverage

| ChartMaze dataset | Current state | Required work |
| --- | --- | --- |
| Horizontal setups | Missing | Detect daily/weekly bases, resistance, length, depth and RVOL. |
| Tight setups | Missing | Detect compressed daily/weekly zones and retain zone coordinates. |
| Inside Bar | Partial | Daily boolean exists; add weekly signals, dates, RVOL and default-condition result. |
| VCP | Missing | Detect progressively smaller swings and publish drawing data. |
| Volume footprint | Missing | Requires volume-at-price data; daily OHLCV cannot reproduce it accurately. |
| Shake out | Missing | Detect support undercut and reclaim against EMA 10/21/50/200. |
| Flag Pattern | Missing | Detect a strong flagpole followed by controlled consolidation. |
| Gap Fill | Partial | Gap percentage exists; add historical open-gap and fill tracking. |
| Positive Earnings Reaction | Partial | Earnings date and returns exist; add volume classification and concall link. |
| Momentum Scanner | Partial | Inputs exist; define and publish one documented selection formula. |
| Circuit Limit Revision | Mostly available | Publish effective date and structured From/To revisions. |
| Block Deals | Source data available | Publish structured records instead of only an event marker. |
| Bulk Deals | Source data available | Publish separately; current event code incorrectly labels bulk as block. |
| Tight Setup Monthly Performance | Missing | Requires retained signal/breakout history and Nifty benchmarking. |
| Inside Bar Monthly Performance | Missing | Requires retained signal/breakout history and Nifty benchmarking. |

## Initial Detector Definitions

All thresholds must be centralized in one configuration object and covered by
tests. These are starting definitions to tune against sample results, not claims
about ChartMaze's private formulas.

- Inside bar: high is no greater than the previous candle's high and low is no
  less than the previous candle's low. Apply independently to daily and resampled
  weekly candles.
- Tight setup: a configurable 5-15 candle window whose high-to-low depth is at
  most roughly 6-10%, with optional declining ATR and volume confirmation.
- Horizontal setup: at least two highs within a configurable tolerance near one
  resistance level, a minimum base length of 15 sessions and controlled depth.
- VCP: at least two or three sequential contractions, each shallower than the
  previous contraction, preferably accompanied by declining volume.
- Shakeout: price trades below a selected EMA but closes back above it. Evaluate
  EMA 10, 21, 50 and 200 independently and optionally require elevated volume.
- Flag: a configurable strong advance over a short period followed by a shorter
  sideways or downward consolidation with limited retracement.
- Gap fill: persist upward and downward candle gaps and mark the first later
  candle that trades back through the open gap boundary.
- Positive earnings reaction: positive earnings-session return or gap, strong
  close and volume above a configurable 20/50-day baseline. Preserve the earnings
  announcement timestamp when selecting the reaction session.
- Momentum: sufficient liquidity, price above SMA 20/50/200, bullish SMA ordering
  and configurable positive 1/3/6-month performance thresholds.
- Volume footprint: defer until an authoritative intraday volume-at-price source
  is available. Do not estimate it from daily volume.

## Output Shape

Pattern details should be structured and explainable rather than expanded into
many unrelated flat fields:

```json
{
  "patterns": {
    "inside_bars": [
      {
        "timeframe": "daily",
        "mother_date": "2026-08-06",
        "signal_date": "2026-08-07",
        "relative_volume": 0.41,
        "default_condition_passed": true
      }
    ],
    "tight_zones": [],
    "horizontal_bases": [],
    "vcp": [],
    "flags": [],
    "gap_fills": [],
    "shakeouts": []
  },
  "earnings_reaction": null,
  "recent_deals": [],
  "circuit_revisions": []
}
```

Each signal should include its timeframe, detection date, input boundaries,
calculated measurements and detector version. Historical signals must not be
discarded if monthly performance will be calculated later.

## Pipeline Design

Add one `pattern_metrics_processor.py` stage after base OHLCV enrichment and
before final standardization:

1. Load a stock's daily OHLCV once.
2. Normalize and validate candle ordering.
3. Resample daily candles into weekly candles once.
4. Run small independent detector functions.
5. Attach structured results to the stock record.
6. Preserve source deal and circuit records separately.
7. Standardize and compress through the existing publication boundary.

Suggested detector boundaries:

```python
detect_inside_bars(daily, weekly, config)
detect_tight_zones(daily, weekly, config)
detect_horizontal_bases(daily, weekly, config)
detect_vcp(daily, config)
detect_flags(daily, config)
detect_gap_fills(daily, config)
detect_shakeouts(daily, config)
detect_earnings_reaction(daily, earnings, config)
detect_momentum(daily, stock, config)
```

Do not make a separate OHLCV pass for every detector. Scanner lists should be
views over the master artifact, for example by requiring both a matching signal
and `default_screener_eligible == true`.

## Delivery Order

### Phase 1: objective signals and source events

- Daily and weekly inside bars.
- Correct separation and publication of bulk and block deals.
- Structured circuit revisions with effective dates.
- Gap tracking and fills.
- EMA shakeouts.
- Positive earnings reaction and earnings-volume classification.

### Phase 2: configurable pattern detectors

- Tight setups.
- Horizontal setups.
- Flag patterns.
- Momentum scanner.
- Compare results against the captured ChartMaze samples and tune documented
  parameters without introducing symbol-specific exceptions.

### Phase 3: complex and historical analysis

- VCP swing detection.
- Persist signal and breakout histories.
- Tight-setup and inside-bar monthly performance.
- Nifty 50 return over the identical stock measurement window.

### Deferred

- Volume footprint until suitable intraday volume-at-price data is available.

## Validation and Regression Requirements

- Unit-test every detector with positive, negative and insufficient-history cases.
- Test daily and weekly candle boundaries, holidays and short trading weeks.
- Test post-market earnings announcements against the next trading session.
- Verify bulk and block deals remain distinct from source through publication.
- Verify all default scanner views exclude SME stocks.
- Verify pattern fields are null or empty when history is insufficient; do not
  publish false certainty as zero or `false`.
- Keep existing canonical price, volume, SMA and fundamental fields unchanged.
- Run the full pipeline and existing test suite before publishing artifacts.
- Compare symbol overlap, false positives and false negatives against each saved
  ChartMaze sample; document differences caused by unknown private conditions.
- Track detector versions so threshold changes do not silently reinterpret old
  historical signals.

## Known Limitations

- Result-only CSV files do not reveal exact Flag, Momentum, Tight, Horizontal,
  VCP or default-condition formulas.
- Daily OHLCV is insufficient for an accurate volume-at-price footprint.
- Concall links may require classification of filings rather than a dedicated
  upstream field.
- Monthly performance cannot be produced reliably until signal and breakout
  history is persisted.

## Retrieval Note

When implementation is requested later, use this document as the source plan,
re-check current pipeline state and upstream schemas, and implement from Phase 1
forward. Do not rely on remembered thresholds if they differ from this file or a
later documented decision.
