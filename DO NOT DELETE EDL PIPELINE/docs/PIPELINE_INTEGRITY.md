# Publication integrity

`python run_full_pipeline.py` and `edl-pipeline` build in an isolated temporary directory. Only the OHLCV cache is shared; old intermediate responses are not reused. Required-stage failure, malformed JSON or a failed quality check leaves the published files unchanged. Successful files, `pipeline_report.json`, and `data_quality.json` are committed together by the daily workflow. Local file promotion rolls back ordinary write failures, but is not atomic for concurrent readers or process termination; consumers requiring atomic releases should read a single Git revision.

The v2 breadth, universe snapshot and index histories now run in the full pipeline and participate in compression/validation. Every JSON record is validated, including non-finite numbers and exponent overflow. NaN/Infinity become null at serialization, and zero earnings-price denominators no longer produce infinity. IPO histories calculate available indicators independently; missing long-period indicators remain null. Missing financial inputs, invalid growth denominators and unavailable legacy breadth cells are null/blank rather than measured zero. These are intentional consumer-visible corrections: consumers must not coerce null/blank to zero.

The final stock universe must match the fetched master (not an independent official NSE universe). A missing fundamental response no longer removes the security; its absent fields remain null. Quality reports list missing fields and historical as-of status for every symbol. At least 90% of stocks and published indices must have history aligned to the NIFTY reference session. All published OHLC records must be coherent, dates must parse, future dates are rejected, and breadth must end at the reference session. New IPOs are not required to have long-history indicators. Individual stale/missing histories are reported, not presented as current.

## Freshness boundaries

- `EDL_EXPECTED_SESSION=YYYY-MM-DD` enforces an exact reference session when the caller knows it.
- Without that setting, the newest published NIFTY bar is the reference. `EDL_MAX_SESSION_AGE_DAYS` defaults to seven calendar days. This is an outage ceiling, **not proof that the latest exchange session is present**.
- Dates use IST for publication checks, independent of the host timezone. An exchange holiday calendar and historical reconciliation remain separate work.
- The three v2 files must have the same generation timestamp from the current IST day. Legacy breadth must share the final reference date.

`EDL_FETCH_OHLCV=0` is now diagnostic-only and never replaces published files. Staging always keeps intermediates until validation and removes its own temporary workspace afterward; `EDL_CLEANUP_INTERMEDIATE` remains relevant when calling the low-level runner directly. Low-level `edl_pipeline.runner.main` is intended for orchestration/testing; use the public entrypoint for protected publication.

Failures write `pipeline_failure_report.json` and, for quality rejection, `data_quality_failure.json`; CI uploads diagnostics even on failure. Shared caches may have progressed even if publication is rejected. Invalid cached history can therefore require source correction/reconciliation before a future run passes; the gate deliberately does not fabricate corrected candles.

Existing field names and artifact names are preserved, including the legacy CSV named `market_breadth.json.gz`. Indicator smoothing conventions, adjusted-price methodology, custom scanner rules, delivery data and source licensing are not changed by this patch.

## Existing-data integration

The canonical stock artifact retains its source `isin` and `security_id`, so consumers can join it safely to reference data. It preserves every current provider index membership and marks it `current_snapshot`; this does not create historical constituent membership.

`data_quality.json` contains coverage for listing date, sector, industry, circuit limit, F&O eligibility, F&O lot size and next expiry. It also contains an explicit per-symbol missing-field list. The pipeline publishes `corporate_action_ledger.json.gz`, a source-preserving ledger of fetched actions. `SPLIT`, `BONUS`, and `RIGHTS` rows are marked `requires_verified_ratio`; `price_adjusted` remains false. Raw OHLCV is deliberately untouched until a reliable factor source and reconciliation policy exist.

F&O expiry matches by Dhan underlying security ID first and normalized symbol second, avoiding punctuation/suffix mismatches. A refresh with eligible F&O securities but zero matched expiries fails rather than publishing an all-empty expiry column.

Verification: run `python -m unittest discover -s tests -v` and `python -m compileall -q .`. The tests include offline real-transform generation plus short-history, missing-input, full-record JSON and rollback cases. Passing tests do not replace a full live refresh against upstream providers.
