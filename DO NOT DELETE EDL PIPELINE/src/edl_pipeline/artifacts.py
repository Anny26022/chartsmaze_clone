"""Artifact names and stage definitions for the pipeline runner."""

from .schemas import EVENT_FIELDS, REQUIRED_FINAL_FIELDS
from .validators import ArtifactSpec

INTERMEDIATE_FILES = [
    "master_isin_map.json",
    "dhan_data_response.json",
    "fundamental_data.json",
    "advanced_indicator_data.json",
    "all_company_announcements.json",
    "upcoming_corporate_actions.json",
    "history_corporate_actions.json",
    "nse_asm_list.json",
    "nse_gsm_list.json",
    "bulk_block_deals.json",
    "upper_circuit_stocks.json",
    "lower_circuit_stocks.json",
    "incremental_price_bands.json",
    "complete_price_bands.json",
    "nse_equity_list.csv",
    "all_stocks_fundamental_analysis.json",
    "sector_analytics.json",
    "market_breadth.csv",
    "etf_data_response.json",
]

INTERMEDIATE_DIRS = [
    "company_filings",
    "market_news",
]

FILES_TO_COMPRESS = {
    "all_stocks_fundamental_analysis.json": "all_stocks_fundamental_analysis.json.gz",
    "sector_analytics.json": "sector_analytics.json.gz",
    "market_breadth.csv": "market_breadth.json.gz",
}

PHASE2_SCRIPTS = [
    "fetch_company_filings.py",
    "fetch_new_announcements.py",
    "fetch_advanced_indicators.py",
    "fetch_market_news.py",
    "fetch_corporate_actions.py",
    "fetch_surveillance_lists.py",
    "fetch_circuit_stocks.py",
    "fetch_bulk_block_deals.py",
    "fetch_incremental_price_bands.py",
    "fetch_complete_price_bands.py",
    "fetch_all_indices.py",
]

PHASE4_SCRIPTS = [
    "advanced_metrics_processor.py",
    "process_earnings_performance.py",
    "enrich_fno_data.py",
    "process_market_breadth.py",
    "process_historical_market_breadth.py",
    "add_corporate_events.py",
]

OPTIONAL_SCRIPTS = [
    "fetch_etf_data.py",
]

SCRIPT_OUTPUT_SPECS = {
    "fetch_dhan_data.py": [
        ArtifactSpec("dhan_data_response.json", "json", min_count=1),
        ArtifactSpec("master_isin_map.json", "json", min_count=1, required_fields=("Symbol", "ISIN", "Sid")),
    ],
    "fetch_fundamental_data.py": [
        ArtifactSpec("fundamental_data.json", "json", min_count=1),
    ],
    "fetch_company_filings.py": [
        ArtifactSpec("company_filings", "dir", min_count=1),
    ],
    "fetch_new_announcements.py": [
        ArtifactSpec("all_company_announcements.json", "json", min_count=0),
    ],
    "fetch_advanced_indicators.py": [
        ArtifactSpec("advanced_indicator_data.json", "json", min_count=0),
    ],
    "fetch_market_news.py": [
        ArtifactSpec("market_news", "dir", min_count=1),
    ],
    "fetch_corporate_actions.py": [
        ArtifactSpec("upcoming_corporate_actions.json", "json", min_count=0),
        ArtifactSpec("history_corporate_actions.json", "json", min_count=0),
    ],
    "fetch_surveillance_lists.py": [
        ArtifactSpec("nse_asm_list.json", "json", min_count=0),
        ArtifactSpec("nse_gsm_list.json", "json", min_count=0),
    ],
    "fetch_circuit_stocks.py": [
        ArtifactSpec("upper_circuit_stocks.json", "json", min_count=0),
        ArtifactSpec("lower_circuit_stocks.json", "json", min_count=0),
    ],
    "fetch_bulk_block_deals.py": [
        ArtifactSpec("bulk_block_deals.json", "json", min_count=0),
    ],
    "fetch_incremental_price_bands.py": [
        ArtifactSpec("incremental_price_bands.json", "json", min_count=0),
    ],
    "fetch_complete_price_bands.py": [
        ArtifactSpec("complete_price_bands.json", "json", min_count=1),
    ],
    "fetch_all_indices.py": [
        ArtifactSpec("all_indices_list.json", "json", min_count=1),
    ],
    "fetch_all_ohlcv.py": [
        ArtifactSpec("ohlcv_data", "dir", min_count=1),
    ],
    "fetch_indices_ohlcv.py": [
        ArtifactSpec("indices_ohlcv_data", "dir", min_count=1),
    ],
    "bulk_market_analyzer.py": [
        ArtifactSpec(
            "all_stocks_fundamental_analysis.json",
            "json",
            min_count=1,
            required_fields=("Symbol", "Name", "Basic Industry", "Sector", "Market Cap(Cr.)"),
        ),
    ],
    "advanced_metrics_processor.py": [
        ArtifactSpec("all_stocks_fundamental_analysis.json", "json", min_count=1),
    ],
    "process_earnings_performance.py": [
        ArtifactSpec("all_stocks_fundamental_analysis.json", "json", min_count=1),
    ],
    "enrich_fno_data.py": [
        ArtifactSpec("all_stocks_fundamental_analysis.json", "json", min_count=1),
    ],
    "process_market_breadth.py": [
        ArtifactSpec("all_stocks_fundamental_analysis.json", "json", min_count=1),
        ArtifactSpec("sector_analytics.json", "json", min_count=1, required_fields=("Sectors", "Industries")),
    ],
    "process_historical_market_breadth.py": [
        ArtifactSpec("market_breadth.csv", "csv", min_count=2),
    ],
    "add_corporate_events.py": [
        ArtifactSpec(
            "all_stocks_fundamental_analysis.json",
            "json",
            min_count=1,
            required_fields=EVENT_FIELDS,
        ),
    ],
    "fetch_etf_data.py": [
        ArtifactSpec("etf_data_response.json", "json", min_count=0),
    ],
}

FINAL_ARTIFACT_SPECS = [
    ArtifactSpec(
        "all_stocks_fundamental_analysis.json.gz",
        "gzip_json",
        min_count=1,
        required_fields=REQUIRED_FINAL_FIELDS,
    ),
    ArtifactSpec("sector_analytics.json.gz", "gzip_json", min_count=1, required_fields=("Sectors", "Industries")),
    ArtifactSpec("market_breadth.json.gz", "gzip_csv", min_count=2),
    ArtifactSpec("all_indices_list.json", "json", min_count=1),
]
