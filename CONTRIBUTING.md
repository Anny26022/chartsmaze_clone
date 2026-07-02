# Contributing

Thanks for improving EDL Pipeline. This project is intentionally script-first because the existing scheduled refresh and downstream consumers depend on the current artifact paths.

## Local Setup

```bash
cd "DO NOT DELETE EDL PIPELINE"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Verification

Run these before opening a pull request:

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q .
```

Only run the full live refresh when your change touches endpoint payloads, pipeline ordering, generated artifact schemas, or data transformations:

```bash
python3 run_full_pipeline.py
```

For faster endpoint-independent checks:

```bash
EDL_FETCH_OHLCV=0 EDL_CLEANUP_INTERMEDIATE=0 python3 run_full_pipeline.py
```

## Contribution Rules

- Preserve existing output field names unless the change is explicitly a schema migration.
- Keep script entrypoints working with `python3 script_name.py`.
- Put shared request, JSON, path, gzip, and parser logic into helper modules instead of duplicating it.
- Do not commit generated intermediate files such as raw JSON, CSV, filings, news folders, or OHLCV caches.
- Document new upstream endpoints in the pipeline README, including payload shape and output files.
- Make endpoint failures explicit. Critical stages should exit non-zero; optional enrichment should report partial data clearly.

## Data Source Etiquette

The project uses public and undocumented web endpoints. Keep request volume reasonable, use bounded concurrency, and do not add authentication bypasses, scraping of private data, or behavior that violates upstream terms.
