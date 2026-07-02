# EDL Pipeline

EDL Pipeline is a Python market-data refresh pipeline for Indian equities. It pulls public Dhan ScanX, Dhan web, and NSE archive data; builds enriched stock analytics; and writes compressed artifacts used by downstream apps.

The working pipeline lives in [`DO NOT DELETE EDL PIPELINE/`](DO%20NOT%20DELETE%20EDL%20PIPELINE/). The folder name is preserved for compatibility with the existing scheduled refresh workflow and generated artifact paths.

## Quick Start

```bash
cd "DO NOT DELETE EDL PIPELINE"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m unittest discover -s tests -v
python3 run_full_pipeline.py
```

For a faster smoke run that skips OHLCV refresh:

```bash
EDL_FETCH_OHLCV=0 python3 run_full_pipeline.py
```

## Outputs

Tracked public artifacts:

- `DO NOT DELETE EDL PIPELINE/all_stocks_fundamental_analysis.json.gz`
- `DO NOT DELETE EDL PIPELINE/sector_analytics.json.gz`
- `DO NOT DELETE EDL PIPELINE/market_breadth.json.gz`
- `DO NOT DELETE EDL PIPELINE/all_indices_list.json`

Most raw JSON, CSV, OHLCV cache, filings, and news files are generated intermediates and are intentionally ignored by Git.

## Development

```bash
cd "DO NOT DELETE EDL PIPELINE"
pip install -r requirements.txt
python3 -m unittest discover -s tests -v
python3 -m compileall -q .
```

Package metadata is provided in `DO NOT DELETE EDL PIPELINE/pyproject.toml`. After installing the folder as a package, the pipeline can also be run with:

```bash
edl-pipeline
```

## Data Disclaimer

This project uses public and undocumented web endpoints. It is not affiliated with Dhan, NSE, or any exchange. Data may be incomplete, delayed, rate-limited, blocked, or structurally changed by upstream providers. See [`docs/DATA_LIMITATIONS.md`](DO%20NOT%20DELETE%20EDL%20PIPELINE/docs/DATA_LIMITATIONS.md) before relying on the generated outputs.

This repository is for data engineering and research workflows only. It is not investment advice.
