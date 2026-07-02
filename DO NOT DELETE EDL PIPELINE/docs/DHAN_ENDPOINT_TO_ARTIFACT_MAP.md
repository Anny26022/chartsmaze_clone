# Endpoint To Artifact Map

This map shows how each public/unofficial upstream source flows through the EDL pipeline. Dhan endpoints are the primary focus, with Google Sheets and NSE sources included because they are also used by the repo.

## Final Artifact Flow

| Source | Script | Raw artifact | Transform/consumer | Final artifact / fields affected |
|---|---|---|---|---|
| Dhan ScanX `customscan/fetchdt` equity scan | `fetch_dhan_data.py` | `dhan_data_response.json`, `master_isin_map.json` | `bulk_market_analyzer.py`, all per-ISIN fetchers | Identity, classification, market cap, price, returns, RSI, index membership, F&O flag |
| Dhan fundamental batch | `fetch_fundamental_data.py` | `fundamental_data.json` | `bulk_market_analyzer.py` | Quarterly metrics, CAGR, valuation, ROE/ROCE, D/E, ownership, float |
| Dhan static company filings | `fetch_company_filings.py` | `company_filings/{SYMBOL}_filings.json` | `process_earnings_performance.py`, `add_corporate_events.py` | Earnings dates/returns, `Recent Announcements`, result markers |
| Dhan static LODR filings | `fetch_company_filings.py` | `company_filings/{SYMBOL}_filings.json` | `process_earnings_performance.py`, `add_corporate_events.py` | Same as company filings, merged and deduped |
| Dhan announcements | `fetch_new_announcements.py` | `all_company_announcements.json` | `add_corporate_events.py` | Event markers and stock news/event context |
| Dhan advanced indicators | `fetch_advanced_indicators.py` | `advanced_indicator_data.json` | `bulk_market_analyzer.py` | `SMA Status`, `EMA Status`, `Technical Sentiment`, `Pivot Point` |
| Dhan live news | `fetch_market_news.py` | `market_news/{SYMBOL}_news.json` | `add_corporate_events.py` | `News Feed` |
| Dhan corporate actions scan | `fetch_corporate_actions.py` | `history_corporate_actions.json`, `upcoming_corporate_actions.json` | `add_corporate_events.py` | Dividend, split, bonus, rights, buyback, result-date markers |
| Dhan deals endpoint | `fetch_bulk_block_deals.py` | `bulk_block_deals.json` | `add_corporate_events.py` | Block/bulk deal event markers |
| Dhan ScanX circuit scan | `fetch_circuit_stocks.py` | `upper_circuit_stocks.json`, `lower_circuit_stocks.json` | `add_corporate_events.py` | Circuit revision/break markers |
| NSE complete price bands | `fetch_complete_price_bands.py` | `complete_price_bands.json` | `advanced_metrics_processor.py`, `add_corporate_events.py` | `Circuit Limit`, price-band event context |
| NSE incremental price bands | `fetch_incremental_price_bands.py` | `incremental_price_bands.json` | `add_corporate_events.py` | Circuit revision markers |
| Dhan tick history | `fetch_all_ohlcv.py` | `ohlcv_data/{SYMBOL}.csv` | `advanced_metrics_processor.py`, `process_market_breadth.py`, `process_historical_market_breadth.py`, `process_earnings_performance.py` | ADR, ATH distance, RVOL, turnover, RS ratings, breadth, earnings returns |
| Dhan index scan | `fetch_all_indices.py` | `all_indices_list.json` | `fetch_indices_ohlcv.py`, breadth processors | Index cache seed, benchmark/index breadth context |
| Dhan index tick history | `fetch_indices_ohlcv.py` | `indices_ohlcv_data/{INDEX}.csv` | `process_historical_market_breadth.py`, `process_market_breadth.py` | Breadth rows, benchmark calculations |
| Dhan Next.js F&O lot size | `enrich_fno_data.py`, `fetch_fno_lot_sizes.py` | `fno_lot_sizes_cleaned.json` when standalone | `enrich_fno_data.py` | `F&O`, `Lot Size` |
| Dhan Next.js F&O expiry | `enrich_fno_data.py`, `fetch_fno_expiry.py` | `fno_expiry_calendar.json` when standalone | `enrich_fno_data.py` | `Next Expiry` |
| Google Sheets Gviz ASM/GSM | `fetch_surveillance_lists.py` | `nse_asm_list.json`, `nse_gsm_list.json` | `add_corporate_events.py` | ASM/GSM surveillance markers |
| Dhan Next.js ASM/GSM fallback | `fetch_surveillance_lists.py` | `nse_asm_list.json`, `nse_gsm_list.json` | `add_corporate_events.py` | Same as Gviz surveillance source |
| NSE listing CSV | runner pre-analysis step | `nse_equity_list.csv` | `bulk_market_analyzer.py` | `Listing Date` |
| Dhan ScanX ETF scan | `fetch_etf_data.py` | `etf_data_response.json` | Standalone optional | Not injected into default final artifact |
| Dhan ScanX F&O flag scan | `fetch_fno_data.py` | `fno_stocks_response.json` | Standalone optional | Not injected directly; default uses `FnoFlag` from master map |

## Endpoint Inventory

| Endpoint / URL pattern | Method | Repo caller(s) | Primary output |
|---|---|---|---|
| `https://ow-scanx-analytics.dhan.co/customscan/fetchdt` | `POST` | `fetch_dhan_data.py`, `fetch_corporate_actions.py`, `fetch_circuit_stocks.py`, `fetch_all_indices.py`, `fetch_all_ohlcv.py`, `fetch_etf_data.py`, `fetch_fno_data.py` | Multiple JSON artifacts |
| `https://open-web-scanx.dhan.co/scanx/fundamental` | `POST` | `fetch_fundamental_data.py` | `fundamental_data.json` |
| `https://ow-static-scanx.dhan.co/staticscanx/company_filings` | `POST` | `fetch_company_filings.py` | `company_filings/` |
| `https://ow-static-scanx.dhan.co/staticscanx/lodr` | `POST` | `fetch_company_filings.py` | `company_filings/` |
| `https://ow-static-scanx.dhan.co/staticscanx/announcements` | `POST` | `fetch_new_announcements.py` | `all_company_announcements.json` |
| `https://ow-static-scanx.dhan.co/staticscanx/indicator` | `POST` | `fetch_advanced_indicators.py` | `advanced_indicator_data.json` |
| `https://news-live.dhan.co/v2/news/getLiveNews` | `POST` | `fetch_market_news.py` | `market_news/` |
| `https://ow-static-scanx.dhan.co/staticscanx/deal` | `POST` | `fetch_bulk_block_deals.py` | `bulk_block_deals.json` |
| `https://openweb-ticks.dhan.co/getDataH` | `POST` | `fetch_all_ohlcv.py`, `fetch_indices_ohlcv.py` | `ohlcv_data/`, `indices_ohlcv_data/` |
| `https://dhan.co/_next/data/{buildId}/nse-fno-lot-size.json` | `GET` | `fetch_fno_lot_sizes.py`, `enrich_fno_data.py` | F&O lot data |
| `https://dhan.co/_next/data/{buildId}/fno-expiry-calendar.json` | `GET` | `fetch_fno_expiry.py`, `enrich_fno_data.py` | F&O expiry data |
| `https://dhan.co/_next/data/{buildId}/nse-asm-list.json` | `GET` | `fetch_surveillance_lists.py` fallback | `nse_asm_list.json` |
| `https://dhan.co/_next/data/{buildId}/nse-gsm-list.json` | `GET` | `fetch_surveillance_lists.py` fallback | `nse_gsm_list.json` |
| `https://dhan.co/_next/data/{buildId}/stocks/market/shares-with-upper-circuit.json` | `GET` | `fetch_circuit_stocks.py` fallback | `upper_circuit_stocks.json` |
| `https://dhan.co/_next/data/{buildId}/stocks/market/lower-circuit-stocks.json` | `GET` | `fetch_circuit_stocks.py` fallback | `lower_circuit_stocks.json` |
| `https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:json&gid=290894275` | `GET` | `fetch_surveillance_lists.py` | `nse_asm_list.json` |
| `https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:json&gid=1525483995` | `GET` | `fetch_surveillance_lists.py` | `nse_gsm_list.json` |
| `https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv` | `GET` | `runner.download_nse_listing_dates()` | `nse_equity_list.csv` |
| `https://nsearchives.nseindia.com/content/equities/eq_band_changes_{date}.csv` | `GET` | `fetch_incremental_price_bands.py` | `incremental_price_bands.json` |
| `https://nsearchives.nseindia.com/content/equities/sec_list_{date}.csv` | `GET` | `fetch_complete_price_bands.py` | `complete_price_bands.json` |

## Build ID Pages

These Dhan pages are fetched only to discover the current Next.js `buildId` or to scrape embedded `__NEXT_DATA__` as fallback:

| Page | Used by | Why |
|---|---|---|
| `https://dhan.co/all-indices/` | `fetch_fno_expiry.py`, `fetch_surveillance_lists.py`, `fetch_circuit_stocks.py` | Build ID seed page |
| `https://dhan.co/nse-fno-lot-size/` | `fetch_fno_lot_sizes.py`, `enrich_fno_data.py` | Build ID seed and lot-size embedded fallback |
| `https://dhan.co/fno-expiry-calendar/` | `fetch_fno_expiry.py` | Expiry embedded fallback |
| `https://dhan.co/nse-asm-list/` | `fetch_surveillance_lists.py` | ASM rendered-page fallback |
| `https://dhan.co/nse-gsm-list/` | `fetch_surveillance_lists.py` | GSM rendered-page fallback |
| `https://dhan.co/stocks/market/shares-with-upper-circuit/` | `fetch_circuit_stocks.py` | Upper-circuit rendered-page fallback |
| `https://dhan.co/stocks/market/lower-circuit-stocks/` | `fetch_circuit_stocks.py` | Lower-circuit rendered-page fallback |

## Consumer Guidance

- Prefer final artifacts over raw endpoint outputs when building downstream tools.
- Use `pipeline_report.json` to check whether the run completed with validation warnings.
- Parse `market_breadth.json.gz` as gzip-compressed CSV, not JSON.
- Treat all raw endpoint fields as unstable unless this repo's transform layer normalizes them into a documented final artifact field.
