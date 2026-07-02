# Unofficial Endpoint Dossier

This document records the public and unofficial endpoints currently used by the EDL pipeline. It is based on the repository implementation, not on any official Dhan, NSE, Google, or exchange API contract.

These endpoints can change, throttle, disappear, or alter response fields without notice. Use this as an engineering reference for this pipeline, not as a guarantee of availability or permission.

## Shared Request Behavior

Most JSON requests use `pipeline_utils.get_headers()`:

```json
{
  "Content-Type": "application/json",
  "User-Agent": "<rotated desktop browser user agent>",
  "Accept": "application/json, text/plain, */*"
}
```

ScanX custom scan calls add:

```json
{
  "Origin": "https://scanx.dhan.co",
  "Referer": "https://scanx.dhan.co/"
}
```

Shared `post_json()` behavior:

- Timeout is set by each caller, usually 10-30 seconds.
- Retries are bounded: default 2 retries with exponential backoff.
- JSON decode shape is validated by the caller, not by the HTTP helper.
- Empty or missing upstream data is often treated as a non-critical pipeline gap.

## 1. Dhan ScanX Custom Scan

| Property | Value |
|---|---|
| URL | `https://ow-scanx-analytics.dhan.co/customscan/fetchdt` |
| Method | `POST` |
| Shared helper | `pipeline_utils.fetch_scanx_data()` |
| Response envelope | JSON object with `data` list |
| Failure behavior | Exceptions propagate to caller; empty/non-list `data` becomes `[]` |

### Purpose In This Repo

This is the broadest Dhan source in the pipeline. The same endpoint is used for equity universe discovery, live snapshots, corporate actions, circuit scans, index discovery, ETF scans, and standalone F&O scans.

### Common Payload Shape

```json
{
  "data": {
    "sort": "Mcap",
    "sorder": "desc",
    "count": 5000,
    "fields": ["Sym", "DispSym", "Isin", "Sid"],
    "params": [
      {"field": "OgInst", "op": "", "val": "ES"},
      {"field": "Exch", "op": "", "val": "NSE"}
    ],
    "pgno": 0
  }
}
```

Important request fields:

- `fields`: exact response fields requested from ScanX.
- `params`: ScanX filters; most filters use empty `op` and a string `val`.
- `count`: requested page size. The repo uses large counts to avoid pagination where possible.
- `pgno`: observed as `0` or `1` depending on source page behavior.
- `sort` / `sorder`: field and direction for upstream sort.

### Equity Universe

| Item | Value |
|---|---|
| Caller | `fetch_dhan_data.py::fetch_all_dhan_data()` |
| Output | `dhan_data_response.json`, `master_isin_map.json` |
| Count | `5000` |
| Filters | `OgInst=ES`, `Exch=NSE` |
| Timeout | 30 seconds |

Consumed response fields include:

- Identity: `Sym`, `DispSym`, `Isin`, `Sid`, `FnoFlag`
- Classification/index membership: `idxlist`
- Market data: `Mcap`, `Ltp`, `Open`, `BcClose`, `volume`
- Valuation: `Pe`, `Pb`, `DivYeild`, `Ind_Pe`, `Eps`
- Technicals/returns: `DayRSI14CurrentCandle`, `DaySMA50CurrentCandle`, `DaySMA200CurrentCandle`, `PricePerchng1week`, `PricePerchng1mon`, `PricePerchng3mon`, `PricePerchng1year`, `High1Yr`

Downstream effects:

- `master_isin_map.json` drives almost every per-stock fetch.
- `dhan_data_response.json` feeds `bulk_market_analyzer.py`, OHLCV sync, and final stock fields such as `Market Cap(Cr.)`, `Stock Price(₹)`, `P/E`, `RSI (14)`, return fields, and index membership.

Known limitations:

- `count=5000` is an observed convenience, not a documented max.
- Missing `Isin` or `Sym` rows are dropped from the master map.
- Field names are upstream-specific and can change.

### Live Stock OHLCV Snapshot

| Item | Value |
|---|---|
| Caller | `fetch_all_ohlcv.py::get_live_snapshots()` |
| Output | Merged into `ohlcv_data/{SYMBOL}.csv` |
| Count | `5000` |
| Filters | `Exch=NSE` |
| Timeout | 15 seconds |

Payload requests `Sym`, `Open`, `High`, `Low`, `Ltp`, and `Volume`, sorted by `Volume`. The response is mapped into today's OHLCV row with `Ltp` as `Close`.

### Corporate Actions

| Item | Value |
|---|---|
| Caller | `fetch_corporate_actions.py::fetch_actions()` |
| Output | `history_corporate_actions.json`, `upcoming_corporate_actions.json` |
| Count | `5000` |
| Filters | Segment, equity universe, date window, market-cap classes, action types |

Payload template:

```json
{
  "data": {
    "sort": "CorpAct.ExDate",
    "sorder": "asc",
    "count": 5000,
    "fields": ["CorpAct.ActType", "Sym", "DispSym", "CorpAct.ExDate", "CorpAct.RecDate", "CorpAct.Note"],
    "params": [
      {"field": "Seg", "op": "", "val": "E"},
      {"field": "OgInst", "op": "", "val": "ES"},
      {"field": "CorpAct.ExDate", "op": "lte", "val": "YYYY-MM-DD"},
      {"field": "CorpAct.ExDate", "op": "gte", "val": "YYYY-MM-DD"},
      {"field": "Mcapclass", "op": "", "val": "Largecap,Midcap,Smallcap,Microcap"},
      {"field": "CorpAct.ActType", "op": "", "val": "BONUS,DIVIDEND,QUARTERLY RESULT ANNOUNCEMENT,SPLIT,RIGHTS,BUYBACK"}
    ],
    "pgno": 0
  }
}
```

Observed response shape:

```json
{
  "data": [
    {
      "Sym": "ABC",
      "DispSym": "ABC Ltd",
      "CorpAct": [
        {"ActType": "DIVIDEND", "ExDate": "2026-01-02", "RecDate": "2026-01-03", "Note": "Rs 1"}
      ]
    }
  ]
}
```

Downstream effects:

- `add_corporate_events.py` converts actions into `Event Markers` and `News Feed` entries.

Known limitations:

- The script applies its own date filtering after receiving nested `CorpAct` rows.
- The two scenarios are hard-coded: two years back for history, 60 days forward for upcoming actions.

### Circuit Stocks

| Item | Value |
|---|---|
| Caller | `fetch_circuit_stocks.py::fetch_circuit_stocks()` |
| Output | `upper_circuit_stocks.json`, `lower_circuit_stocks.json` |
| Count | `500` |
| Primary source | ScanX custom scan |
| Fallback | Dhan Next.js JSON, then page scrape |

Upper circuit filter:

```json
{"field": "LiveData.UpperCircuitBreak", "op": "", "val": "1"}
```

Lower circuit filter:

```json
{"field": "LiveData.LowerCircuitBreak", "op": "", "val": "1"}
```

Additional filters: `OgInst=ES`, `Seg=E`.

Consumed response fields:

- `Sym`, `DispSym`, `Ltp`, `PPerchange`, `Mcap`, `Volume`, `High5yr`, `High1Yr`, `Low1Yr`, `Pe`, `Pb`, `DivYeild`

Downstream effects:

- Circuit files contribute `#` circuit revision/event markers in `add_corporate_events.py`.

### Index Discovery

| Item | Value |
|---|---|
| Caller | `fetch_all_indices.py::fetch_all_indices()` |
| Output | `all_indices_list.json` |
| Count | `500` |
| Filters | `Inst=IDX`, `Exch=IDX` |
| Timeout | 15 seconds |

Consumed response fields:

- `Sym`, `DispSym`, `Sid`, `Exch`, `Seg`, `Inst`
- `Open`, `High`, `Low`, `Ltp`, `Pchange`, `PPerchange`
- `High1Yr`, `Low1Yr`, `Min1TotalVolPrevCandle`

Downstream effects:

- Drives `fetch_indices_ohlcv.py`.
- Provides current-day index snapshot rows.
- Feeds historical breadth calculations.

### ETF Scan

| Item | Value |
|---|---|
| Caller | `fetch_etf_data.py::fetch_all_etf_data()` |
| Output | `etf_data_response.json` |
| Count | `1000` |
| Filters | `OgInst=ETF`, `Exch=NSE` |
| Runner phase | Optional |

This endpoint is standalone and does not affect the default final stock artifact.

### F&O Flag Scan

| Item | Value |
|---|---|
| Caller | `fetch_fno_data.py::fetch_fno_flag_data()` |
| Output | `fno_stocks_response.json` |
| Count | `500` |
| Filters | `FnoFlag=1`, `OgInst=ES` |
| Runner phase | Standalone |

This is a standalone scan. The default F&O enrichment uses `FnoFlag` from `master_isin_map.json`, then augments with Next.js lot-size and expiry data.

## 2. Dhan Fundamental Batch Endpoint

| Property | Value |
|---|---|
| URL | `https://open-web-scanx.dhan.co/scanx/fundamental` |
| Method | `POST` |
| Caller | `fetch_fundamental_data.py::fetch_fundamental_data()` |
| Output | `fundamental_data.json` |
| Headers | Standard JSON/browser headers |
| Batch size | 100 ISINs |
| Delay | 0.5 seconds between batches |
| Timeout | 30 seconds |

Payload template:

```json
{
  "data": {
    "isins": ["INE000A01000", "INE111A01000"]
  }
}
```

Observed response envelope:

```json
{
  "status": "success",
  "data": [
    {
      "isin": "INE000A01000",
      "incomeStat_cq": {},
      "incomeStat_cy": {},
      "TTM_cy": {},
      "CV": {},
      "roce_roe": {},
      "sHp": {},
      "bs_c": {}
    }
  ]
}
```

Key response groups consumed:

- `incomeStat_cq`: quarterly profit, EPS, sales, OPM, quarter labels.
- `incomeStat_cy`: annual EPS and sales.
- `TTM_cy`: trailing EPS and OPM.
- `CV`: industry, sector, market cap, stock PE.
- `roce_roe`: ROE and ROCE.
- `sHp`: shareholding values for FII, DII, promoter.
- `bs_c`: liabilities and equity for debt/equity.

Downstream effects:

- `bulk_market_analyzer.py` converts this into fundamental, valuation, ownership, and growth fields in `all_stocks_fundamental_analysis.json`.

Known limitations:

- Some ISIN batches can return fewer records than requested.
- If `status` is not `success`, the batch is skipped.
- Missing nested metrics become default values in the final analyzer.

## 3. Dhan Static ScanX Company Filings

| Property | Value |
|---|---|
| URL | `https://ow-static-scanx.dhan.co/staticscanx/company_filings` |
| Method | `POST` |
| Caller | `fetch_company_filings.py::fetch_endpoint()` |
| Output | `company_filings/{SYMBOL}_filings.json` |
| Threads | 20 |
| Timeout | 10 seconds |

Payload template:

```json
{
  "data": {
    "isin": "INE000A01000",
    "pg_no": 1,
    "count": 100
  }
}
```

Observed response envelope:

```json
{
  "data": [
    {
      "news_id": "123",
      "news_date": "2026-01-01",
      "caption": "Financial Results",
      "descriptor": "Results",
      "file_url": "https://..."
    }
  ]
}
```

Downstream effects:

- Merged with LODR filings.
- Used by earnings processing and event/news enrichment.

Known limitations:

- Only page 1 with `count=100` is fetched.
- Empty responses are common and counted as non-fatal errors by the script.
- Records are deduplicated by `news_id` or by date plus caption.

## 4. Dhan Static ScanX LODR Filings

| Property | Value |
|---|---|
| URL | `https://ow-static-scanx.dhan.co/staticscanx/lodr` |
| Method | `POST` |
| Caller | `fetch_company_filings.py::fetch_endpoint()` |
| Output | Merged into `company_filings/{SYMBOL}_filings.json` |
| Threads | Shared 20-thread filings pool |
| Timeout | 10 seconds |

Payload is identical to company filings:

```json
{
  "data": {
    "isin": "INE000A01000",
    "pg_no": 1,
    "count": 100
  }
}
```

Observed response shape is similar to company filings and is deduped into the same output file.

Downstream effects:

- Improves regulatory filing coverage for `Recent Announcements`, earnings date detection, and event markers.

## 5. Dhan Static ScanX Announcements

| Property | Value |
|---|---|
| URL | `https://ow-static-scanx.dhan.co/staticscanx/announcements` |
| Method | `POST` |
| Caller | `fetch_new_announcements.py::fetch_announcements()` |
| Output | `all_company_announcements.json` |
| Threads | 40 |
| Timeout | 10 seconds |

Payload template:

```json
{
  "data": {
    "isin": "INE000A01000"
  }
}
```

Observed response envelope:

```json
{
  "data": [
    {
      "events": "Board meeting",
      "date": "2026-01-01",
      "type": "Announcement"
    }
  ]
}
```

The pipeline normalizes rows into:

```json
{
  "Symbol": "ABC",
  "Name": "ABC Ltd",
  "Event": "Board meeting",
  "Date": "2026-01-01",
  "Type": "Announcement"
}
```

Downstream effects:

- Used by `add_corporate_events.py` for event/news enrichment.

Known limitations:

- Empty responses are normal for many stocks.
- Per-ISIN failures return `None` and do not stop the script.

## 6. Dhan Static ScanX Indicator Endpoint

| Property | Value |
|---|---|
| URL | `https://ow-static-scanx.dhan.co/staticscanx/indicator` |
| Method | `POST` |
| Caller | `fetch_advanced_indicators.py::fetch_indicators()` |
| Output | `advanced_indicator_data.json` |
| Threads | 50 |
| Timeout | 10 seconds |

Payload template:

```json
{
  "exchange": "NSE",
  "segment": "E",
  "security_id": "12345",
  "isin": "INE000A01000",
  "symbol": "ABC",
  "minute": "D"
}
```

Observed response envelope:

```json
{
  "data": [
    {
      "EMA": [],
      "SMA": [],
      "Indicator": [],
      "Pivot": []
    }
  ]
}
```

Normalized output:

```json
{
  "Symbol": "ABC",
  "EMA": [],
  "SMA": [],
  "TechnicalIndicators": [],
  "Pivots": []
}
```

Downstream effects:

- Feeds `SMA Status`, `EMA Status`, `Technical Sentiment`, and `Pivot Point`.

Known limitations:

- Requires `Sid` from `master_isin_map.json`.
- Some valid symbols can return no indicator list.

## 7. Dhan Live News

| Property | Value |
|---|---|
| URL | `https://news-live.dhan.co/v2/news/getLiveNews` |
| Method | `POST` |
| Caller | `fetch_market_news.py::fetch_market_news()` |
| Output | `market_news/{SYMBOL}_news.json` |
| Threads | 15 |
| Timeout | 10 seconds |
| Per-stock limit | 50 |

Payload template:

```json
{
  "categories": ["ALL"],
  "page_no": 0,
  "limit": 50,
  "first_news_timeStamp": 0,
  "last_news_timeStamp": 0,
  "news_feed_type": "live",
  "stock_list": ["INE000A01000"],
  "entity_id": ""
}
```

Observed response envelope:

```json
{
  "data": {
    "latest_news": [
      {
        "publish_date": 1783000000000,
        "category": "markets",
        "news_object": {
          "title": "Headline",
          "text": "Summary",
          "overall_sentiment": "positive"
        }
      }
    ]
  }
}
```

Normalized output:

```json
{
  "Symbol": "ABC",
  "ISIN": "INE000A01000",
  "News": [
    {
      "Title": "Headline",
      "Summary": "Summary",
      "Sentiment": "positive",
      "PublishDate": 1783000000000,
      "Source": "markets"
    }
  ]
}
```

Downstream effects:

- `add_corporate_events.py` converts this into the final `News Feed` field.

Known limitations:

- `429` is handled with a 2-second sleep but not retried inside the same task.
- Empty news is normal for some symbols.

## 8. Dhan Static ScanX Deals

| Property | Value |
|---|---|
| URL | `https://ow-static-scanx.dhan.co/staticscanx/deal` |
| Method | `POST` |
| Caller | `fetch_bulk_block_deals.py::fetch_bulk_block_deals()` |
| Output | `bulk_block_deals.json` |
| Page size | 50 |
| Date window | Three 10-day chunks, 30 days total |
| Timeout | 10 seconds |

Payload template:

```json
{
  "data": {
    "startdate": "23-06-2026",
    "enddate": "02-07-2026",
    "defaultpage": "N",
    "pageno": 1,
    "pagecount": 50
  }
}
```

Observed response envelope:

```json
{
  "data": [
    {
      "sym": "ABC",
      "date": "2026-07-01",
      "qty": 100000,
      "avgprice": 123.45,
      "bs": "B",
      "cname": "Buyer"
    }
  ],
  "totalcount": 1234
}
```

Pagination:

- First page response `totalcount` determines `ceil(totalcount / 50)`.
- The endpoint rejects or limits date spans larger than roughly 240 hours, so the script chunks by 10 days.

Downstream effects:

- Recent deals add `📦: Block Deal` style event markers.

Known limitations:

- The script stops a chunk on the first non-200 response or exception.
- Deals are deduped by symbol/date/quantity/price/buy-sell/customer composite key.

## 9. Dhan Tick History

| Property | Value |
|---|---|
| URL | `https://openweb-ticks.dhan.co/getDataH` |
| Method | `POST` |
| Callers | `fetch_all_ohlcv.py::fetch_history_chunk()`, `fetch_indices_ohlcv.py::fetch_chunk()` |
| Outputs | `ohlcv_data/{SYMBOL}.csv`, `indices_ohlcv_data/{INDEX}.csv` |
| Stock chunk size | 180 days |
| Index chunk size | 120 days |
| Stock threads | 15 |
| Index threads | 60 |

Payload template:

```json
{
  "EXCH": "NSE",
  "SYM": "ABC",
  "SEG": "E",
  "INST": "EQUITY",
  "SEC_ID": 12345,
  "EXPCODE": 0,
  "INTERVAL": "D",
  "START": 1719878400,
  "END": 1783000000
}
```

Observed response envelope:

```json
{
  "data": {
    "Time": [1719878400],
    "o": [100.0],
    "h": [105.0],
    "l": [99.0],
    "c": [103.0],
    "v": [1000000]
  }
}
```

Normalized CSV columns:

```csv
Date,Open,High,Low,Close,Volume
```

Downstream effects:

- OHLCV cache feeds ADR, ATH distance, relative volume, post-earnings returns, RS ratings, market breadth, and historical breadth.

Known limitations:

- No official historical depth guarantee.
- Existing CSV cache controls incremental start date.
- Current-day rows are patched from ScanX live snapshots, not this historical endpoint.

## 10. Dhan Next.js Build ID And Data Files

| Property | Value |
|---|---|
| Public page pattern | `https://dhan.co/<page>/` |
| Data URL pattern | `https://dhan.co/_next/data/{buildId}/{page_path}.json` |
| Method | `GET` |
| Helpers | `dhan_next_utils.get_build_id()`, `get_next_data()`, `get_embedded_next_data()` |
| Headers | User-Agent only |

Build ID extraction:

- Fetch a rendered Dhan page.
- Regex search for `"buildId":"..."`.
- Use the build ID in `_next/data` JSON URLs.

Generic observed response shape:

```json
{
  "pageProps": {
    "...": "page-specific data"
  },
  "__N_SSG": true
}
```

### F&O Lot Size

| Item | Value |
|---|---|
| Public page | `https://dhan.co/nse-fno-lot-size/` |
| Data path | `nse-fno-lot-size` |
| Data URL | `https://dhan.co/_next/data/{buildId}/nse-fno-lot-size.json` |
| Callers | `fetch_fno_lot_sizes.py`, `enrich_fno_data.py` |
| Output | `fno_lot_sizes_cleaned.json`; also enriches final master JSON |

Observed data path:

```json
{
  "pageProps": {
    "listData": [
      {
        "sym": "ABC",
        "disp": "ABC Ltd",
        "fo_dt": [
          {"sym": "ABC-JUL", "ls": 75}
        ]
      }
    ]
  }
}
```

Downstream effects:

- Adds `Lot Size` for F&O stocks in final master JSON.

Fallback:

- If direct `_next/data` is empty, `fetch_fno_lot_sizes.py` parses the page `__NEXT_DATA__` script and searches nested lists.

### F&O Expiry Calendar

| Item | Value |
|---|---|
| Build ID source page | `https://dhan.co/all-indices/` or lot-size page in enrichment |
| Public page | `https://dhan.co/fno-expiry-calendar/` |
| Data path | `fno-expiry-calendar` |
| Data URL | `https://dhan.co/_next/data/{buildId}/fno-expiry-calendar.json` |
| Callers | `fetch_fno_expiry.py`, `enrich_fno_data.py` |
| Output | `fno_expiry_calendar.json`; also enriches final master JSON |

Observed data path:

```json
{
  "pageProps": {
    "expiryData": {
      "data": [
        {
          "exch": "NSE",
          "seg": "D",
          "exps": [
            {
              "inst": "FUT",
              "explst": [
                {
                  "symbolName": "ABC",
                  "expdate": "2026-07-30",
                  "underlyingSecID": 123
                }
              ]
            }
          ]
        }
      ]
    }
  }
}
```

Downstream effects:

- Adds `Next Expiry` for F&O stocks in final master JSON.

### ASM/GSM Surveillance Pages

| Item | ASM | GSM |
|---|---|---|
| Public page | `https://dhan.co/nse-asm-list/` | `https://dhan.co/nse-gsm-list/` |
| Data path | `nse-asm-list` | `nse-gsm-list` |
| Data URL | `https://dhan.co/_next/data/{buildId}/nse-asm-list.json` | `https://dhan.co/_next/data/{buildId}/nse-gsm-list.json` |
| Caller | `fetch_surveillance_lists.py` | `fetch_surveillance_lists.py` |
| Output | `nse_asm_list.json` | `nse_gsm_list.json` |

The current primary source is Google Sheets Gviz. Dhan Next.js JSON is the second fallback, followed by scraping the rendered page or `/index.html`.

Normalized output:

```json
{
  "Symbol": "ABC",
  "Name": "ABC Ltd",
  "ISIN": "INE000A01000",
  "Stage": "I"
}
```

Downstream effects:

- Adds `★: LTASM` / `★: STASM` style surveillance markers.

### Circuit Pages

| Item | Upper Circuit | Lower Circuit |
|---|---|---|
| Public page | `https://dhan.co/stocks/market/shares-with-upper-circuit/` | `https://dhan.co/stocks/market/lower-circuit-stocks/` |
| Data path | `stocks/market/shares-with-upper-circuit` | `stocks/market/lower-circuit-stocks` |
| Caller | `fetch_circuit_stocks.py` | `fetch_circuit_stocks.py` |
| Output | `upper_circuit_stocks.json` | `lower_circuit_stocks.json` |

Dhan Next.js JSON is only used if the primary ScanX circuit scan fails.

## 11. Google Sheets Gviz Surveillance Source

| Property | Value |
|---|---|
| URL pattern | `https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:json&gid={gid}` |
| Method | `GET` |
| Caller | `fetch_surveillance_lists.py::fetch_surveillance_lists()` |
| Output | `nse_asm_list.json`, `nse_gsm_list.json` |
| Role | Primary surveillance source |

GIDs:

- ASM: `290894275`
- GSM: `1525483995`

Observed response format:

```text
google.visualization.Query.setResponse({...});
```

The script extracts the JSON inside `setResponse(...)`, reads `table.rows`, and maps columns:

- `c[1]`: symbol
- `c[2]`: name
- `c[3]`: ISIN
- `c[4]`: stage

Known limitations:

- This is not a Dhan endpoint, but it is the repo's primary surveillance source.
- The spreadsheet ID, GIDs, and column order are undocumented dependencies.
- If Gviz fails, the script falls back to Dhan Next.js JSON and then Dhan page scraping.

## 12. NSE Archive CSV Sources

These are not Dhan endpoints, but they are public upstream sources used by the repo.

### Listing Dates

| Property | Value |
|---|---|
| URL | `https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv` |
| Method | `GET` via `curl` |
| Caller | `runner.download_nse_listing_dates()` |
| Output | `nse_equity_list.csv` |
| Downstream | `Listing Date` field in final stock artifact |

### Incremental Price Band Changes

| Property | Value |
|---|---|
| URL pattern | `https://nsearchives.nseindia.com/content/equities/eq_band_changes_{date}.csv` |
| Date format | `DDMMYYYY` |
| Method | `GET` |
| Caller | `fetch_incremental_price_bands.py` |
| Output | `incremental_price_bands.json` |

### Complete Security Price Bands

| Property | Value |
|---|---|
| URL pattern | `https://nsearchives.nseindia.com/content/equities/sec_list_{date}.csv` |
| Date format | `DDMMYYYY` |
| Method | `GET` |
| Caller | `fetch_complete_price_bands.py` |
| Output | `complete_price_bands.json` |
| Downstream | `Circuit Limit`, advanced metrics, price-band revision markers |

`nse_archive_utils.fetch_latest_nse_csv()` looks back across recent dates and returns the first CSV that parses successfully.

## Operational Notes For Builders

- Treat every endpoint here as source-dependent and unstable.
- Do not assume response field completeness. The pipeline intentionally tolerates missing enrichment data.
- Prefer consuming final artifacts rather than calling upstream endpoints directly.
- If you call upstream endpoints directly, keep concurrency bounded and preserve ordinary browser-like headers.
- Include run date and validation status from `pipeline_report.json` when publishing derived data.
