# Chart publication to R2

The scanner and chart files share one release manifest: `frontend/public/data/current.json`.
It contains the scanner revision, session date, immutable scanner/IPO URLs, chart revision,
and chart URL template. Per-scanner `release.json` files let open screens keep their revision.
There is no separate mutable R2 current pointer.

## Configuration

GitHub Actions requires secrets `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, and
`R2_SECRET_ACCESS_KEY`, plus repository variable `R2_PUBLIC_BASE_URL` (the HTTPS
public/custom-domain delivery URL for `nexus-screener-chart-data`). Configure
cross-origin GET access for the frontend origin. These credentials must have
read/write access because publication verifies uploads and archives prior releases.
Missing configuration or upload failure stops publication before the Git commit.
Local development defaults to an ignored `/data/charts/<chart-revision>/` cache.
Chart bytes are `application/gzip`, without `Content-Encoding: gzip`; the browser
explicitly decompresses them. Cache immutable chart URLs, but bypass CDN/browser
caching for `/data/current.json`.

## Publication

The pipeline builds charts after canonical financial/filing ledgers and before
compression or intermediate cleanup. Staged chart files are preserved during
publication; the workflows upload these files without rebuilding them after news
inputs have been discarded. Scanner data is exported from the same pipeline run.
They verify chart count, payload symbol/session and content revision before uploading.
Uploads are checked against their source, and charts never enter frontend Git revisions.
The shared manifest is written only after upload and any required month-end archive succeed.
Git publishes the scanner snapshot and pointer together. R2 stores only immutable objects.
Reruns use checksums and preserve the original release publication timestamp.

## Retention

- `daily/<session>/<chart-revision>/`: expire after **90 calendar days from upload**.
- `monthly/<YYYY-MM>/<chart-revision>/`: keep indefinitely.
- The first successful publication in a new month archives the previous month's
  latest successful trading session, including its chart index and release metadata.
  Weekends/holidays require no calendar-day guess. If a month has no successful
  publication, there is no fabricated month-end release.
- Daily/weekly runs are serialized with the same workflow concurrency group.

The daily lifecycle rule has been applied to the dedicated bucket:

```sh
wrangler r2 bucket lifecycle add nexus-screener-chart-data daily-charts-90-days daily/ --expire-days 90 --force
```

This is 90 calendar days, not 90 trading sessions. Lifecycle deletion is asynchronous.
Historical daily chart links expire; month-end charts remain at their monthly URLs.
Legacy `revisions/` objects are outside the new expiration rule, including the initial
September 2026 upload. No existing objects were deleted when adding the rule.
If publication stops for over 90 days, daily objects can expire before rollover archival;
the rule does not protect the last current release through an indefinite outage.

At 47 MB per full revision, the rolling daily storage is approximately 4.2 GB,
plus approximately 0.56 GB for each year of month-end releases (before corrections).
