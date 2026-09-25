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

This is a current daily snapshot. A historical daily delivery series still
requires archived daily files or a licensed bulk/EOD source.

## Collection

Run the collector before the full EDL pipeline. It makes one manifest request
and one CSV download, then stages `nse_delivery_data.json` for the normal EDL
run to merge automatically.

```bash
cd "DO NOT DELETE EDL PIPELINE"
python fetch_nse_delivery_data.py
python run_full_pipeline.py
```

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
