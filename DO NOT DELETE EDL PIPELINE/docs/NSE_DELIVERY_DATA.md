# NSE delivery data

The scanner can publish these optional, per-symbol fields:

- `delivery_percent`: `COP_DELIV_PERC` / percentage of traded quantity marked deliverable.
- `deliverable_quantity`: `COP_DELIV_QTY`.
- `delivery_traded_quantity`: `CH_TOT_TRADED_QTY`.
- `delivery_as_of_date` and `delivery_series`.

## Source and contract

The adapter uses the endpoint called by NSE's own **Security-wise Archives
(Equities)** page:

`GET /api/historicalOR/generateSecurityWiseHistoricalData?from=DD-MM-YYYY&to=DD-MM-YYYY&symbol=RELIANCE&type=priceVolumeDeliverable&series=ALL`

It is a symbol-scoped public page API, not a bulk-data service. The pipeline
therefore does **not** issue thousands of requests as part of every full run.
For production universe-wide historical delivery coverage, ingest a licensed
bulk/EOD source into `nse_delivery_data.json` using the documented schema.

## Bounded collection

Use the adapter only for a small, explicit set of symbols, with a conservative
delay. It writes a staging file that the normal EDL run merges automatically.

```bash
cd "DO NOT DELETE EDL PIPELINE"
python fetch_nse_delivery_data.py \
  --symbols-file symbols.json \
  --from-date 18-09-2026 --to-date 25-09-2026 \
  --max-symbols 50 --delay-seconds 0.5
python run_full_pipeline.py
```

`symbols.json` may be a JSON list of symbols or the existing master ISIN-map
format. The command fails rather than exceeding its explicit safety cap.

## Staging schema

```json
{
  "source": "NSE security-wise price-volume-deliverable archive",
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
