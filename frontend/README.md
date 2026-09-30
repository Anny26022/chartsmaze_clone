# Nexus Research Screener — Frontend Application

An original, professional quantitative research screener for NSE mainboard equities built with React 19, Vite, TypeScript, React Query, and Tailwind CSS.

---

## 🏛️ System Architecture & Design Philosophy

The Nexus Research Screener is designed ground-up for equity research analysts and quantitative traders requiring point-in-time scanning, transparent condition compilation, and data-integrity safeguards across NSE mainboard stocks.

### Key Architectural Highlights
1. **Zero Direct `useEffect` Policy**: Follows strict React 19 and repo-wide guidelines. All side-effects and data loading are managed via TanStack React Query (`useQuery`, `useMutation`), event handlers, or derived state.
2. **Data Integrity & Point-in-Time Session Guarantees**:
   - Displays explicit **"As of [NSE session date]"** header and table badges.
   - Handles missing historical attributes with explicit **"Unavailable for this date"** warnings and diagnostics.
   - Renders missing sector/industry values strictly as **"Unclassified"** (never infers classifications from company names).
   - Omits raw full-universe bulk data exports to protect infrastructure.
3. **Dual Screening Workflows**:
   - **Visual Condition Builder**: Interactive parameter editors with searchable condition catalog and Match All (AND) / Match Any (OR) modes.
   - **Nexus Query Language (NQL)**: Custom open boolean text syntax with nested group support and human-readable explanation compilation.
4. **Tabbed Workflows**:
   - **Explore Tab**: Mainboard screener workspace with universe selector (Mainboard, Nifty 50, Nifty 500, MidSmall 400), as-of date selector, active filter chips, and paginated results table.
   - **New Listings (IPOs) Tab**: Filterable window for newly listed equities and IPO performance metrics (issue price, listing price, return since listing, data completeness).
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

By default, the application runs with an in-memory **Local Mock Adapter** (`src/api/mockAdapter.ts`) pre-populated with realistic NSE mainboard stock data, IPO catalogs, and historical session diagnostics.

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
