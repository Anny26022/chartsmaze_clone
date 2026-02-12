# EDL Pipeline - Dhan ScanX Data Integration

This directory contains the tools and data for fetching stock market analytics from the **Dhan ScanX** API.

## 🚀 API Documentation

### 1. Fetch Full Market Data (`NSE Total Market`)
Used to retrieve detailed data for all available stocks in the NSE market.

*   **Endpoint**: `https://ow-scanx-analytics.dhan.co/customscan/fetchdt`
*   **Method**: `POST`
*   **Total Records**: ~2,775 unique symbols.

### 2. Fetch F&O Stock List (`FnoFlag: 1`)
Used to retrieve the precise universe of 207 F&O stocks.

*   **Endpoint**: `https://ow-scanx-analytics.dhan.co/customscan/fetchdt`
*   **Method**: `POST`
*   **Request Parameter**: `{"field": "FnoFlag", "op": "", "val": "1"}`
*   **Total Records**: **207** (Exact F&O universe).
*   **Pagination**: All 207 records are returned in a single call if `count` is set to 500.

### 3. Fetch F&O Lot Sizes
Extracted from the Dhan lot size page to provide the contract sizes for indices and stocks.

*   **Source**: `https://dhan.co/nse-fno-lot-size/`
*   **Total Records**: 212 instruments.
*   **Format**: Maps symbol and name to lot sizes for the current and future months.

## 🛠 Project Files

| File | Description |
| :--- | :--- |
| `fetch_dhan_data.py` | Python script to fetch the full NSE market (2,775 symbols). |
| `dhan_data_response.json` | Latest output for the full market fetch. |
| `fetch_fno_data.py` | Python script to fetch the exact 207 F&O stocks. |
| `fno_stocks_response.json` | Latest output for the 207 F&O stocks. |
| `fetch_fno_lot_sizes.py` | Python script to fetch and parse lot sizes. |
| `fno_lot_sizes_cleaned.json` | Data containing lot sizes for all F&O instruments. |
| `fetch_all_indices.py` | Python script to fetch all market indices (NSE/BSE). |
| `all_indices_list.json` | List of all market indices with their IDs and symbols. |
| `fetch_etf_data.py` | Python script to fetch all ETFs. |
| `etf_data_response.json` | Latest output for the all-ETF fetch. |
| `fetch_fno_expiry.py` | Python script to fetch the F&O expiry calendar. |
| `fno_expiry_calendar.json` | Cleaned data for upcoming F&O expiry dates. |
| `fetch_corporate_actions.py` | Python script to fetch both scenarios into separate files. |
| `history_corporate_actions.json` | 2 years of past events (Bonus, Dividend, etc.). |
| `upcoming_corporate_actions.json` | 2 months of future tracked events. |
| `fetch_surveillance_lists.py` | Python script to fetch NSE ASM/GSM lists. |
| `nse_asm_list.json` | Stocks under NSE Additional Surveillance Measure. |
| `nse_gsm_list.json` | Stocks under NSE Graded Surveillance Measure. |
| `fetch_circuit_stocks.py` | Python script to fetch Upper/Lower circuit stocks. |
| `upper_circuit_stocks.json` | Stocks hitting Upper Circuit limit. |
| `lower_circuit_stocks.json` | Stocks hitting Lower Circuit limit. |
| `fetch_incremental_price_bands.py` | Script for incremental daily price band changes. |
| `incremental_price_bands.json` | Latest daily price band updates (changes only). |
| `fetch_complete_price_bands.py` | Script for the complete list of price bands. |
| `complete_price_bands.json` | Complete list of all securities and their bands. |
| `fetch_bulk_block_deals.py` | Script to fetch Bulk/Block deals (last 30 days). |
| `bulk_block_deals.json` | Consolidated list of bulk and block deals. |
| `README.md` | This documentation file. |

## 📂 Data Content & Field Reference

### 1. `dhan_data_response.json` & `fno_stocks_response.json`
*   **Content**: Technical and fundamental data for 2,775 stocks (Full) or 207 stocks (F&O).
*   **Key Fields**: 
    *   `Sym`, `DispSym`, `Isin`: Identifiers.
    *   `Ltp`, `volume`: Price and liquidity.
    *   `Mcap`, `Pe`, `Pb`, `Roe`, `ROCE`, `DivYeild`: Fundamental ratios.
    *   `DaySMA50CurrentCandle`, `DaySMA200CurrentCandle`: Trend indicators.
    *   `DayRSI14CurrentCandle`: Momentum indicator.
    *   `High1Yr`, `High3Yr`, `High5yr`: Historical benchmarks.

### 2. `fno_lot_sizes_cleaned.json`
*   **Content**: Derivative contract lot sizes for active instruments.
*   **Key Fields**:
    *   `Symbol`, `Name`: Instrument identity.
    *   `Lot_Feb2026`, `Lot_Mar2026`, etc.: Specific lot sizes for upcoming monthly expiries.

### 3. `all_indices_list.json`
*   **Content**: Master list of 194 indices across NSE and BSE.
*   **Key Fields**:
    *   `IndexName`, `Symbol`, `IndexID`: Unique mapping.
    *   `Exchange`, `BasedOnExch`: Connectivity details.
    *   `Ltp`, `Chng`, `PChng`: Live performance.

### 4. `etf_data_response.json`
*   **Content**: Detailed performance and cost data for 361 ETFs.
*   **Key Fields**:
    *   `ExpenseRatio`: The cost of holding the ETF.
    *   `Mcap`, `Volume`, `Ltp`: Asset size and liquidity.
    *   `RtAwayFrom5YearHigh`: Percentage drop from 5-year peak.

### 5. `fno_expiry_calendar.json`
*   **Content**: Schedule of all future expiry dates.
*   **Key Fields**:
    *   `InstrumentType`: `FUTIDX`, `FUTSTK`, `OPTIDX`, etc.
    *   `SymbolName`: Specific contract name.
    *   `ExpiryDate`: The settlement date (YYYY-MM-DD).

### 6. `corporate_actions_response.json`
*   **Content**: History and upcoming events for the **past 2 years** and **next 2 months** (Dynamic IST).
*   **Key Fields**:
    *   `CorpAct.ActType`: `DIVIDEND`, `SPLIT`, `BONUS`, `BUYBACK`, `RIGHTS`, etc.
    *   `CorpAct.ExDate`: The ex-date for the event.
    *   `Sym`, `PPerchange`: Stock symbol and recent movement.

## ⚙️ How to Update Data

**To refresh Full Market Data:**
```bash
python3 "fetch_dhan_data.py"
```

**To refresh F&O Data (207 Stocks):**
```bash
python3 "fetch_fno_data.py"
```

**To refresh F&O Lot Sizes:**
```bash
python3 "fetch_fno_lot_sizes.py"
```

**To refresh All Indices List:**
```bash
python3 "fetch_all_indices.py"
```

**To refresh ETF List:**
```bash
python3 "fetch_etf_data.py"
```

**To refresh F&O Expiry Calendar:**
```bash
python3 "fetch_fno_expiry.py"
```

**To refresh Corporate Actions (Last 2 Years):**
```bash
python3 "fetch_corporate_actions.py"
```

**To refresh Surveillance Lists (ASM/GSM):**
```bash
python3 "fetch_surveillance_lists.py"
```

**To refresh Circuit Limit Stocks:**
```bash
python3 "fetch_circuit_stocks.py"
```

**To refresh Incremental Price Band Changes (Daily):**
```bash
python3 "fetch_incremental_price_bands.py"
```

**To refresh Complete Price Band List (All Securities):**
```bash
python3 "fetch_complete_price_bands.py"
```

**To refresh Bulk & Block Deals (Last 30 Days):**
```bash
python3 "fetch_bulk_block_deals.py"
```

---
**Note**: This folder is part of the EDL Pipeline. **DO NOT DELETE**.
