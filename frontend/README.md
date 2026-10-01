# Nexus Research Screener — Frontend Application

An original, professional quantitative research screener for NSE mainboard equities built with React 19, Vite, TypeScript, React Query, and Tailwind CSS.

---

## 🏛️ System Architecture & Design Philosophy

The Nexus Research Screener screens NSE mainboard equities against the latest published trading session.

### Key Architectural Highlights
1. **Zero Direct `useEffect` Policy**: Follows strict React 19 and repo-wide guidelines. All side-effects and data loading are managed via TanStack React Query (`useQuery`, `useMutation`), event handlers, or derived state.
2. **Versioned data**: The browser and Python evaluator use the same immutable scanner revision. Missing inputs remain unavailable and do not pass filters. Missing classifications display as "Unclassified".
3. **Dual Screening Workflows**:
   - **Visual Condition Builder**: Interactive parameter editors with searchable condition catalog and Match All (AND) / Match Any (OR) modes.
   - **Nexus Query Language (NQL)**: Custom open boolean text syntax with nested group support and human-readable explanation compilation.
4. **Tabbed Workflows**:
   - **Explore Tab**: Mainboard screener workspace with universe selector (Mainboard, Nifty 50, Nifty 500, MidSmall 400), active filter chips, and paginated results table.
   - **New Listings (IPOs) Tab**: Filterable window for newly listed equities and listing date, current price, turnover, delivery and sector/industry.
   - **Symbol List Tab**: Ticker normalizer for pasted symbol lists with validation diagnostics and custom list scanning.

---

## 📡 API Contracts (`https://screener-api.nexusjournal.co.in/v1`)

The frontend communicates with a typed backend API interface (`src/api/screenerApi.ts`):

- **`GET /v1/catalog`**: Retrieves available indicator definitions, parameters, and metadata.
- **`POST /v1/screens/explain`**: Validates expression tree / NQL text query and compiles human-readable explanations and session availability warnings.
- **`POST /v1/screens/run`**: Executes screen across specified universe and session date. Returns resolved session ID, immutable data revision, paginated rows, match count, per-condition coverage, and diagnostics.
- **`GET /v1/ipos`**: Fetches new listings catalog.
- **`POST /v1/symbols/compare`**: Normalizes and validates pasted ticker lists.
- **`GET /v1/revisions/current`**: Retrieves current active session date and immutable revision ID.

---

## 🛠️ Local Development & Mock Mode

By default, supported filters and all 45 preset defaults run in the browser
against a versioned snapshot. Custom history calculations use the Python EDL
evaluator through Vite's `POST /api/screens/run`. The mock adapter is opt-in
with `VITE_USE_MOCK=true`.

`publish_snapshot.py` calculates full-precision metrics and preset results once,
freezes Python inputs locally, and writes `public/data/current.json` last. The
manifest identifies immutable files under `public/data/revisions/<revision>/`.
The browser checks it every minute, on focus, and on Run Screen. Same-session
corrections receive a new content revision. Filters, IPO data, and backend
requests use that revision, so updates cannot mix old and new datasets.

The validated EDL publication automatically runs the snapshot publisher. Both
refresh workflows commit the browser manifest and versioned files. Private
Python inputs stay under EDL `.scanner_cache/revisions/`; deploy those with the
Python backend when serving custom history calculations. Git does not carry
those caches. After a local Git pull/history update, publish the local snapshot:

```bash
python3 publish_snapshot.py
```

Python retains a worker and numeric history cache for custom calculations.

The catalog includes all 45 public JournalToday presets and the published condition catalog.
Presets require market cap above ₹1,000 Cr, price above ₹10 and 50-day average turnover above ₹5 Cr. They have no upper market-cap/price limits or blanket 2%/5% circuit exclusions.
Missing data produces unavailable diagnostics instead of passing a condition.
Current exported snapshot metrics and quarterly financial statements are available;
historical technical screens require the EDL `ohlcv_data` cache. Intraday turnover
needs intraday data. Exact remote RS rankings and proprietary pattern results are
not verified. Past dates are rejected when the stock history cache is absent.

### Restore and update local history

The history folders are ignored by Git and retained separately by GitHub Actions.
Pulling the repository does not download them. Restore a local EDL cache by ISIN,
then apply official NSE daily files for the missing sessions:

```bash
cd "../DO NOT DELETE EDL PIPELINE"
python3 sync_local_scanner_history.py --source-root "/path/to/existing/EDL" --from-date 2026-09-29 --as-of-date 2026-09-30
```

For subsequent daily updates, omit `--source-root` and supply the missing date
range. This writes local CSVs and `local_scanner_history_report.json`; it preserves
existing history. New listings still need 50 recorded sessions for SMA50 and
21 sessions for RVOL20 (today plus the preceding 20). Missing session candles
remain unavailable instead of passing a filter. Publish the new snapshot after
this standalone history update, then click Run Screen.

Run locally at http://localhost:8080:

```bash
npm run dev -- --host localhost --port 8080 --strictPort
```

A static production deployment must provide a backend implementing
`POST /screens/run` at `VITE_API_BASE_URL`, or proxy `/api/screens/run` to the
Python service. The Vite development middleware is not included in static output.

### Commands
```bash
# Install dependencies
npm install

# Start Vite dev server
npm run dev

# Run Vitest test suite
npm run test

# Compile TypeScript and build production bundle
npm run build
```

### Switching to Production API
Set the environment variable in `.env`:
```env
VITE_USE_MOCK=false
VITE_API_BASE_URL=https://screener-api.nexusjournal.co.in/v1
```
