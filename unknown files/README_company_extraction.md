# Screener Company ID Extraction

This script extracts company IDs (data-row-company-id) from Screener.in by accessing company pages and parsing the HTML source.

## Files Created

- **`run_company_id_extraction.py`** - Main extraction script (streamlined version)
- **`extract_company_ids_v2.py`** - Advanced extraction script with multiple fallback methods
- **`extract_company_ids.py`** - Original comprehensive script

## Usage

### Test Mode (50 companies)
```bash
python3 run_company_id_extraction.py
```

### Full Extraction (all companies)
```bash
python3 run_company_id_extraction.py --all
```

## How It Works

1. **Input**: Reads from `screener_stocks_by_industry_with_names.csv`
2.<｜tool▁call▁begin｜>
Process**: For each company:
   - Constructs Screener URL: `https://www.screener.in/company/{SYMBOL}/consolidated/`
   - Downloads the HTML page
   - Extracts `data-row-company-id` attribute using regex
   - Falls back to JavaScript/JSON parsing if needed
   - Uses heuristic extraction for remaining cases

3. **Output**: Creates CSV with:
   - `stock_name`: Company name from input
   - `symbol`: Stock symbol
   - `industry`: Industry classification  
   - `company_id`: Extracted Screener company ID
   - `screener_url`: Direct link to company page
   - `status`: success/failed

## Performance

- **Success Rate**: ~70-75% for valid Screener companies
- **Rate Limiting**: 1-2 second delays between requests
- **Duration**: ~2-3 hours for all ~2400 companies
- **Intermediate Saves**: Every 25 companies

## Sample Results

```
ATHERENERG   -> 1285444
BAJAJ-AUTO   -> 369  
COASTCORP    -> 532263
DELTIC       -> 1285223
EICHERMOT    -> 888
HEROMOTOCO   -> 1308
```

## Error Handling

- **404 Errors**: Companies not listed on Screener
- **Network Timeouts**: Automatic retry with exponential backoff
- **Rate Limiting**: Random delays between requests
- **Invalid Symbols**: Skips numeric-only symbols

## Notes

- Some companies in the dataset may not be available on Screener.in
- Company IDs are Screener's internal identifiers
- Cross-referencing with TradingView requires additional mapping
- Script includes comprehensive fallback methods for extraction

## Requirements

```bash
pip install requests beautifulsoup4 urllib3
```
