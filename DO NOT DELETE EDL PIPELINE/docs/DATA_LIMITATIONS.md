# Data Sources and Limitations

This pipeline is a data engineering workflow around public web endpoints. It is not an official market-data feed and is not affiliated with Dhan, NSE, or any exchange.

## Upstream Sources

The pipeline currently uses:

- Dhan ScanX public/custom scan endpoints
- Dhan static ScanX endpoints
- Dhan public Next.js data endpoints
- Dhan public news endpoints
- NSE archive CSV endpoints
- Google Sheets Gviz endpoints as a fallback for some surveillance lists

These endpoints are public or publicly reachable, but several are undocumented. Payloads, field names, rate limits, response formats, and availability can change without notice.

## Reliability Limits

- A successful run means the scripts completed and produced artifacts. It does not guarantee market-data completeness.
- Optional enrichment sources can fail or return empty data while the core stock artifact still builds.
- Network failures, DNS failures, throttling, upstream cache delays, and HTML/Next.js build changes can affect output.
- Source coverage changes over time, so stock counts, sector counts, and field coverage can move between runs.
- Some values are derived from locally cached OHLCV data. A stale or partial cache can affect ATH, ADR, relative volume, and breadth metrics.
- `market_breadth.json.gz` is a legacy-compatible gzip artifact that currently contains CSV-formatted breadth data despite the `.json.gz` suffix.

## Known Data Gaps

- Advanced indicator responses can occasionally return zero usable records from the live endpoint.
- Bulk/block deals can be unavailable when the source endpoint or DNS resolution fails.
- `Historical P/E 5` is retained as a schema field but currently has limited upstream support.
- Industry short-period rank fields are currently calculated from available relative-strength data and should be treated as approximate until historical rank logic is expanded.
- Some historical breadth rows include placeholders where upstream or historical inputs are unavailable.

## Usage Guidance

- Treat generated outputs as research data, not as trading, tax, legal, or investment advice.
- Validate critical fields against official exchange/company filings before making decisions.
- Keep request concurrency bounded and avoid adding behavior that bypasses access controls or violates upstream terms.
- If publishing generated artifacts, include the run date and a clear note that data is best-effort and source-dependent.
