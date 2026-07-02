"""Canonical artifact paths used by package code and tests."""

from dataclasses import dataclass
from pathlib import Path

from pipeline_utils import BASE_PATH, resolve_path


@dataclass(frozen=True)
class ArtifactPaths:
    base_dir: Path = BASE_PATH
    master_json: Path = BASE_PATH / "all_stocks_fundamental_analysis.json"
    master_gzip: Path = BASE_PATH / "all_stocks_fundamental_analysis.json.gz"
    sector_json: Path = BASE_PATH / "sector_analytics.json"
    sector_gzip: Path = BASE_PATH / "sector_analytics.json.gz"
    breadth_csv: Path = BASE_PATH / "market_breadth.csv"
    breadth_gzip: Path = BASE_PATH / "market_breadth.json.gz"
    stock_ohlcv_dir: Path = BASE_PATH / "ohlcv_data"
    index_ohlcv_dir: Path = BASE_PATH / "indices_ohlcv_data"


def artifact_path(path):
    return resolve_path(path)
