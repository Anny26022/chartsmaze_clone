# Screener.in Stock Symbol Scraper

This scraper extract stock symbols and industry information from [Screener.in](https://www.screener.in/market/) by navigating through all industries and collecting company data.

## Features

- 🏢 Scrapes all industries from Screener.in market overview
- 🏭 Extracts company names and stock symbols from each industry
- 📊 Outputs data in requested format: `Stock Name,Basic Industry`
- 🚀 Includes rate limiting and error handling
- 💾 Saves data in both CSV and TXT formats

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Option 1: Simple Runner Script
```bash
python run_scraper.py
```
This will interactively ask for confirmation before starting the scraping process.

### Option 2: Direct Script Execution
```bash
python screener_scraper.py
```
This runs the scraper directly without user prompts.

## Output Files

The scraper creates two output files:

1. **screener_stocks.csv** - Detailed format with columns:
   - stock_name
   - symbol
   - industry
   - url

2. **screener_stocks.txt** - Simple format as requested:
   ```
   BAJAJ-AUTO,Auto Manufacturers
   EICHERMOT,2/3 Wheelers
   MARUTI,Auto Manufacturers
   ```

## How It Works

1. **Fetches Industry List**: Scrapes the market overview page to get all industry URLs
2. **Industry Navigation**: For each industry, visits the industry page to get company listings
3. **Symbol Extraction**: Extracts stock symbols from company URLs (e.g., `EICHERMOT` from `/company/EICHERMOT/consolidated/`)
4. **Data Collection**: Combines company names with their respective industries
5. **Output Generation**: Saves data in both detailed CSV and simple TXT formats

## Example Output

```
Stock Name,Basic Industry
BAJAJ-AUTO,Auto Manufacturers
EICHERMOT,2/3 Wheelers
MARUTI,Auto Manufacturers
TATA-MOTORS,Auto Manufacturers
HERO-MOTO,2/3 Wheelers
TVS-MOTOR,2/3 Wheelers
```

## Rate Limiting

The scraper includes built-in delays to be respectful to Screener.in:
- 1 second delay between page requests
- 2 seconds delay between industry processing

## Error Handling

- Network timeouts and retries
- Graceful handling of missing data
- Logging of all operations for debugging

## Requirements

- Python 3.6+
- Internet connection
- Required packages listed in `requirements.txt`

## Notes

- This scraper is intended for educational/research purposes
- Please respect Screener.in's robots.txt and terms of service
- Consider adding longer delays if running at scale
- The scraper outputs company names as they appear on Screener.in

## Troubleshooting

If you encounter issues:
1. Check your internet connection
2. Ensure all dependencies are installed
3. Check the console output for error messages
4. Try running with longer delays if getting rate-limited

