# EDL Pipeline — Dhan ScanX Data Integration

> **Single command to refresh everything:** `python3 run_full_pipeline.py`

---

## 🚀 Master Pipeline Runner

```bash
python3 run_full_pipeline.py
```

Runs **16 scripts** in the correct dependency order and produces `all_stocks_fundamental_analysis.json` in ~4 minutes.

**Configuration flags** (edit inside the script):
- `FETCH_OHLCV = True/False` — Include lifetime OHLCV download (~30 min extra).
- `FETCH_OPTIONAL = True/False` — Include FNO, ETF, Indices standalone data.

### Pipeline Phases
```
PHASE 1 (Core):       fetch_dhan_data.py → fetch_fundamental_data.py
PHASE 2 (Enrichment): fetch_company_filings.py, fetch_market_news.py, etc.
PHASE 3 (Analysis):   bulk_market_analyzer.py (creates base JSON)
PHASE 4 (Injection):  advanced_metrics_processor.py → process_earnings_performance.py → add_corporate_events.py (LAST!)
```

⚠️ **Rule**: `bulk_market_analyzer.py` MUST run before Phase 4. `add_corporate_events.py` MUST be the very last script.

---

## 📡 API Reference (Endpoints, Payloads & Limits)

### 1. Full Market Data — `fetch_dhan_data.py`
| Key | Value |
|---|---|
| **URL** | `https://ow-scanx-analytics.dhan.co/customscan/fetchdt` |
| **Method** | `POST` |
| **Page Size** | `count: 5000` (returns all ~2,775 in one call) |
| **Output** | `dhan_data_response.json` + `master_isin_map.json` |

```json
{
  "data": {
    "type": "full", "whichpage": "nse_total_market",
    "filters": [], "sort": "Mcap", "sorder": "desc",
    "count": 5000, "page": 1
  }
}
```

### 2. Fundamental Data (Results & Ratios) — `fetch_fundamental_data.py`
| Key | Value |
|---|---|
| **URL** | `https://open-web-scanx.dhan.co/scanx/fundamental` |
| **Method** | `POST` |
| **Pagination** | Per-ISIN (iterates `master_isin_map.json`) |
| **Timeout** | 30s |
| **Output** | `fundamental_data.json` (35 MB) |

```json
{"data": {"isin": "<ISIN>"}}
```

### 3. Company Filings (Hybrid) — `fetch_company_filings.py`
Fetches from **TWO** endpoints and merges results for maximum coverage.

| Key | Value |
|---|---|
| **URL 1** | `https://ow-static-scanx.dhan.co/staticscanx/company_filings` |
| **URL 2** | `https://ow-static-scanx.dhan.co/staticscanx/lodr` |
| **Method** | `POST` |
| **Page Size** | `count: 100, pg_no: 1` |
| **Threads** | 20 |
| **Dedup** | By `news_id` + `news_date` + `caption` |
| **Output** | `company_filings/{SYMBOL}_filings.json` |

```json
{"data": {"isin": "<ISIN>", "pg_no": 1, "count": 100}}
```

### 4. Live Announcements — `fetch_new_announcements.py`
| Key | Value |
|---|---|
| **URL** | `https://ow-static-scanx.dhan.co/staticscanx/announcements` |
| **Method** | `POST` |
| **Threads** | 40 |
| **Output** | `all_company_announcements.json` |

```json
{"data": {"isin": "<ISIN>"}}
```

### 5. Advanced Indicators (Pivot, EMA, SMA) — `fetch_advanced_indicators.py`
| Key | Value |
|---|---|
| **URL** | `https://ow-static-scanx.dhan.co/staticscanx/indicator` |
| **Method** | `POST` |
| **Threads** | 50 |
| **Requires** | `Sid` (Security ID from `master_isin_map.json`) |
| **Output** | `advanced_indicator_data.json` (8.3 MB) |

```json
{
  "exchange": "NSE", "segment": "E",
  "security_id": "<Sid>", "isin": "<ISIN>",
  "symbol": "<SYMBOL>", "minute": "D"
}
```

### 6. Market News Feed — `fetch_market_news.py`
| Key | Value |
|---|---|
| **URL** | `https://news-live.dhan.co/v2/news/getLiveNews` |
| **Method** | `POST` |
| **Page Size** | `limit: 50` (per stock) |
| **Max Tested** | `limit: 100` works, pagination via `page_no` |
| **Threads** | 15 |
| **Output** | `market_news/{SYMBOL}_news.json` |

```json
{
  "categories": ["ALL"], "page_no": 0, "limit": 50,
  "first_news_timeStamp": 0, "last_news_timeStamp": 0,
  "news_feed_type": "live",
  "stock_list": ["<ISIN>"], "entity_id": ""
}
```

### 7. Corporate Actions — `fetch_corporate_actions.py`
| Key | Value |
|---|---|
| **URL** | `https://ow-scanx-analytics.dhan.co/customscan/fetchdt` |
| **Method** | `POST` |
| **Page Size** | `count: 5000` |
| **Modes** | History (2 years back) + Upcoming (2 months ahead) |
| **Output** | `history_corporate_actions.json`, `upcoming_corporate_actions.json` |

```json
{
  "data": {
    "type": "full", "whichpage": "corporate_action",
    "filters": [{"field": "CorpAct.ActDate", "op": "GT", "val": "<DATE>"}],
    "count": 5000, "page": 1
  }
}
```

### 8. Surveillance Lists (ASM/GSM) — `fetch_surveillance_lists.py`
| Key | Value |
|---|---|
| **URL** | Google Sheets Gviz endpoint (fallback: Dhan Next.js API) |
| **Method** | `GET` |
| **Output** | `nse_asm_list.json`, `nse_gsm_list.json` |

### 9. Circuit Stocks — `fetch_circuit_stocks.py`
| Key | Value |
|---|---|
| **URL** | `https://ow-scanx-analytics.dhan.co/customscan/fetchdt` |
| **Method** | `POST` |
| **Page Size** | `count: 500` |
| **Output** | `upper_circuit_stocks.json`, `lower_circuit_stocks.json` |

### 10. Bulk/Block Deals — `fetch_bulk_block_deals.py`
| Key | Value |
|---|---|
| **URL** | `https://ow-static-scanx.dhan.co/staticscanx/deal` |
| **Method** | `POST` |
| **Page Size** | `pagecount: 50` (auto-paginates all pages) |
| **Output** | `bulk_block_deals.json` |

```json
{"data": {"defaultpage": "N", "pageno": 1, "pagecount": 50}}
```

### 11. Price Bands — `fetch_incremental_price_bands.py` / `fetch_complete_price_bands.py`
| Key | Value |
|---|---|
| **URL (Incremental)** | `https://nsearchives.nseindia.com/content/equities/eq_band_changes_{date}.csv` |
| **URL (Complete)** | `https://nsearchives.nseindia.com/content/equities/sec_list_{date}.csv` |
| **Method** | `GET` (CSV download) |
| **Output** | `incremental_price_bands.json`, `complete_price_bands.json` |

### 12. Historical OHLCV — `fetch_all_ohlcv.py`
| Key | Value |
|---|---|
| **URL** | `https://openweb-ticks.dhan.co/getDataH` |
| **Method** | `POST` |
| **Threads** | 15 |
| **Start** | `215634600` (Oct 31, 1976 — forces max history) |
| **Interval** | `D` (Daily candles) |
| **Output** | `ohlcv_data/{SYMBOL}.csv` |

```json
{
  "EXCH": "NSE", "SYM": "<SYMBOL>", "SEG": "E", "INST": "EQUITY",
  "SEC_ID": "<Sid>", "EXPCODE": 0,
  "INTERVAL": "D", "START": 215634600, "END": <CURRENT_TIMESTAMP>
}
```

---

## 📂 Standalone Scripts (Not in Pipeline)

| Script | URL | Output |
|---|---|---|
| `fetch_fno_data.py` | `customscan/fetchdt` (filter: `FnoFlag=1`, count: 500) | `fno_stocks_response.json` |
| `fetch_fno_lot_sizes.py` | `dhan.co/nse-fno-lot-size/` (Next.js data) | `fno_lot_sizes_cleaned.json` |
| `fetch_fno_expiry.py` | `dhan.co/_next/data/{buildId}/fno-expiry-calendar.json` | `fno_expiry_calendar.json` |
| `fetch_all_indices.py` | `customscan/fetchdt` + Gviz fallback (count: 500) | `all_indices_list.json` |
| `fetch_etf_data.py` | `customscan/fetchdt` (filter: `ETFFlag`, count: 1000) | `etf_data_response.json` |

> **Note**: Run these manually when needed. Their output JSONs are **not** consumed by the pipeline.

---

## 🛠 Project Files

### Core Pipeline Scripts
| File | Role |
|---|---|
| `run_full_pipeline.py` | **Master Runner** — single command to produce everything |
| `fetch_dhan_data.py` | Fetches 2,775 stocks → `dhan_data_response.json` + `master_isin_map.json` |
| `fetch_fundamental_data.py` | Fetches quarterly results & ratios → `fundamental_data.json` |
| `fetch_company_filings.py` | Hybrid filing engine (LODR + Legacy) → `company_filings/` |
| `fetch_new_announcements.py` | Live corporate announcements → `all_company_announcements.json` |
| `fetch_advanced_indicators.py` | Pivot Points, EMA/SMA signals → `advanced_indicator_data.json` |
| `fetch_market_news.py` | AI-sentiment news (50/stock) → `market_news/` |
| `fetch_corporate_actions.py` | Dividends, Bonus, Splits → `upcoming/history_corporate_actions.json` |
| `fetch_surveillance_lists.py` | ASM/GSM lists → `nse_asm_list.json`, `nse_gsm_list.json` |
| `fetch_circuit_stocks.py` | Upper/Lower circuit → `upper/lower_circuit_stocks.json` |
| `fetch_bulk_block_deals.py` | Bulk/Block deals (30 days) → `bulk_block_deals.json` |
| `fetch_incremental_price_bands.py` | Daily price band changes → `incremental_price_bands.json` |
| `fetch_complete_price_bands.py` | All securities bands → `complete_price_bands.json` |
| `bulk_market_analyzer.py` | Builds base `all_stocks_fundamental_analysis.json` |
| `advanced_metrics_processor.py` | Injects ADR, RVOL, ATH, Turnover |
| `process_earnings_performance.py` | Injects post-earnings returns |
| `add_corporate_events.py` | Injects Event Markers, Announcements, News Feed (FINAL) |
| `single_stock_analyzer.py` | Utility to inspect a single stock |

### Standalone Scripts
| File | Role |
|---|---|
| `fetch_fno_data.py` | 207 F&O stocks |
| `fetch_fno_lot_sizes.py` | F&O lot sizes |
| `fetch_fno_expiry.py` | Expiry calendar |
| `fetch_all_indices.py` | 194 market indices |
| `fetch_etf_data.py` | 361 ETFs |
| `fetch_all_ohlcv.py` | Lifetime OHLCV history (optional in pipeline) |

---

## 📊 Output Field Reference (`all_stocks_fundamental_analysis.json`)

**Total: 86 fields per stock across 2,775 stocks.**

### 1. Identity & Classification
`Symbol`, `Name`, `Listing Date`, `Basic Industry`, `Sector`, `Index`

### 2. Fundamentals (Quarterly)
`Latest Quarter`, `Net Profit Latest/Previous/2Q/3Q/LastYr Quarter`, `EPS Latest/Previous/2Q/3Q/LastYr Quarter`, `Sales Latest/Previous/2Q/3Q/LastYr Quarter`, `OPM Latest/Previous/2Q/3Q/LastYr Quarter`, `QoQ %` and `YoY %` for all, `Sales Growth 5 Years(%)`

### 3. Valuation Ratios
`Market Cap(Cr.)`, `Stock Price(₹)`, `P/E`, `Forward P/E`, `Historical P/E 5`, `PEG`, `ROE(%)`, `ROCE(%)`, `D/E`, `OPM TTM(%)`, `EPS Last Year`, `EPS 2 Years Back`

### 4. Ownership & Float
`FII % change QoQ`, `DII % change QoQ`, `Free Float(%)`, `Float Shares(Cr.)`

### 5. Technical Indicators
| Field | Example |
|---|---|
| `RSI (14)` | `62.5` |
| `SMA Status` | `SMA 20: Above (4.9%) \| SMA 50: Above (24.1%)` |
| `EMA Status` | `EMA 20: Above (6.3%) \| EMA 200: Above (72.6%)` |
| `Technical Sentiment` | `RSI: Neutral \| MACD: Bearish` |
| `Pivot Point` | `245.50` |

### 6. Price Performance
| Field | Description |
|---|---|
| `1 Day/Week/Month/3M/6M/1Y Returns(%)` | Period returns |
| `% from 52W High` | Distance from 52-week peak |
| `% from 52W Low` | Distance from 52-week bottom |
| `% from ATH` | Distance from All-Time High |
| `Gap Up %` | Today's gap |
| `Day Range(%)` | Intraday high-low spread |

### 7. Volume & Liquidity
| Field | Description |
|---|---|
| `RVOL` | Relative Volume (vs 20-day avg) |
| `200 Days EMA Volume` | Long-term volume trend |
| `% from 52W High 200 Days EMA Volume` | Volume trend vs peak |
| `Daily Rupee Turnover 20/50/100(Cr.)` | Turnover moving averages |
| `30 Days Average Rupee Volume(Cr.)` | Monthly volume |

### 8. Volatility
`5/14/20/30 Days MA ADR(%)` — Average Daily Range over different periods.

### 9. Circuit & Price Bands
`Circuit Limit` — Current circuit limit band (e.g., `20%`).

### 10. Earnings Tracking
| Field | Description |
|---|---|
| `Quarterly Results Date` | Date of the latest financial results filing |
| `Returns since Earnings(%)` | % change from pre-earnings close to current price |
| `Max Returns since Earnings(%)` | Peak % gain since results day |

### 11. Event Markers (`Event Markers` field)
| Icon | Name | Trigger |
|---|---|---|
| **★: LTASM / STASM** | Surveillance | Stock in ASM groups |
| **📊: Results Recently Out** | Results | Results released in last 7 days |
| **🔑: Insider Trading** | Insider | SEBI Reg 7(2) / Form C in last 15 days |
| **📦: Block Deal** | Deals | Bulk/Block deal in last 7 days |
| **#: +/- Revision** | Circuit | Price band revision detected |
| **⏰: Results (DD-Mon)** | Upcoming | Results upcoming (with date) |
| **🎁: Bonus (DD-Mon)** | Bonus | Upcoming bonus (with date) |
| **✂️: Split (DD-Mon)** | Split | Upcoming split (with date) |
| **💸: Dividend (DD-Mon)** | Dividend | Upcoming dividend (with date) |
| **📈: Rights (DD-Mon)** | Rights | Upcoming rights issue (with date) |

### 12. Recent Announcements (Regulatory)
`Recent Announcements` — Top 5 regulatory filings with `Date`, `Headline`, `URL` (PDF link).

### 13. News Feed (Media)
`News Feed` — Top 5 real-time news items with `Title`, `Sentiment` (positive/negative/neutral), `Date`.

---
**Note**: This folder is part of the EDL Pipeline. **DO NOT DELETE**.
