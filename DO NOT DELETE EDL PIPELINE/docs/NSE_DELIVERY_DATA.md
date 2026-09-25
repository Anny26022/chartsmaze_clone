# NSE delivery data

The scanner can publish these optional, per-symbol fields:

- `delivery_percent`: `DELIV_PER` / percentage of traded quantity marked deliverable.
- `deliverable_quantity`: `DELIV_QTY`.
- `delivery_traded_quantity`: `TTL_TRD_QNTY`.
- `delivery_as_of_date` and `delivery_series`.

## Source and contract

The adapter first reads NSE's own daily-report manifest:

`GET /api/daily-reports?key=CM`

It resolves NSE's `CM-BHAVDATA-FULL` entry—for example
`https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_25092026.csv`—and
downloads one CSV containing the full daily NSE delivery universe. The API
response supplies the filename and URL, so the pipeline does not guess paths.

The same official archive also supplies one `sec_bhavdata_full_DDMMYYYY.csv`
file per published session.  The pipeline retains a local 260-session cache,
which is enough for the scanner's 252-session / 52-week rules and lets
`Delivery % Spike` evaluate its `fired_within` window from date-aligned rows.

## Collection

Run the collector before the full EDL pipeline. It makes one manifest request
and one CSV download, then stages `nse_delivery_data.json` for the normal EDL
run to merge automatically.

```bash
cd "DO NOT DELETE EDL PIPELINE"
python fetch_nse_delivery_data.py
python run_full_pipeline.py
```

`run_full_pipeline.py` also runs `fetch_nse_delivery_history.py`. The history
cache is deliberately local and ignored by Git: it contains hundreds of
full-universe CSV-derived files, and is reused by later refreshes rather than
downloaded again. Run it separately to build or repair the cache:

```bash
python fetch_nse_delivery_history.py --sessions 260
```

The collector records old 404s as non-published dates (weekends and holidays),
but retries the latest seven calendar days because the current bhavcopy may
appear after the first request. Network or NSE server failures are reported as
failures and never misclassified as a non-trading day.

## Staging schema

```json
{
  "source": "NSE daily full bhavcopy and security deliverable data",
  "records": [{
    "symbol": "RELIANCE",
    "series": "EQ",
    "date": "2026-09-25",
    "traded_quantity": 13138735,
    "deliverable_quantity": 8311348,
    "delivery_percent": 63.26
  }]
}
```

Missing records remain `null`; they are never represented as zero or silently
copied from a different session.
