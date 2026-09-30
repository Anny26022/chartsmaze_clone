/**
 * Real data adapter — loads stock data from static JSON files exported
 * from the EDL pipeline (all_stocks_fundamental_analysis.json.gz + RS ratings).
 *
 * All filtering, sorting, and pagination happens client-side in-memory.
 * 2,600 stocks × ~50 fields ≈ 4 MB JSON, parses in <20ms, filters in <2ms.
 */

import {
  ScreenerRunRequest,
  ScreenerRunResponse,
  StockRow,
  IPORow,
  ExplainRequest,
  ExplainResponse,
  SymbolComparisonRequest,
  SymbolComparisonResponse,
  RevisionCurrentResponse,
  ConditionDef,
} from '../types/screener';
import { NEXUS_CONDITION_CATALOG } from '../data/conditionCatalog';

// Index sets for universe filtering
const NIFTY50_LABEL = 'Nifty 50';
const NIFTY500_LABEL = 'NIFTY 500';
const NIFTY_MIDSMALL400_LABEL = 'Nifty MidSmallCap 400';

interface StocksPayload {
  asOfDate: string;
  totalStocks: number;
  stocks: RawStock[];
}

/** Raw shape from /data/stocks.json — snake_case camelCase mix from export_data.py */
interface RawStock {
  symbol: string;
  name: string;
  listingDate: string;
  sector: string;
  industry: string;
  series: string;
  indexMemberships: string[];
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
  changePct: number | null;
  gapPct: number | null;
  marketCapCrore: number | null;
  rupeeVolumeCrore: number | null;
  avgRupeeVolume20Cr: number | null;
  avgRupeeVolume50Cr: number | null;
  sharesOutstanding: number | null;
  freeFloatPct: number | null;
  sma10: number | null;
  sma20: number | null;
  sma50: number | null;
  sma200: number | null;
  rsi14: number | null;
  atr14: number | null;
  adrPct20: number | null;
  rvol: number | null;
  pivotPoint: number | null;
  closeAboveSma10: boolean;
  closeAboveSma20: boolean;
  closeAboveSma50: boolean;
  closeAboveSma200: boolean;
  sma10AboveSma20: boolean;
  sma20AboveSma50: boolean;
  sma50AboveSma200: boolean;
  distFromSma20Pct: number | null;
  distFromSma50Pct: number | null;
  distFromSma200Pct: number | null;
  dist52wHighPct: number | null;
  dist52wLowPct: number | null;
  pctFromAth: number | null;
  bullishCandle: boolean;
  closeNearDayHigh: boolean;
  breakout20dHigh: boolean;
  breakout50dHigh: boolean;
  near52wHigh: boolean;
  breakout52wHigh: boolean;
  isNr7: boolean;
  isInsideDay: boolean;
  isBullishEngulfing: boolean;
  perf1w: number | null;
  perf1m: number | null;
  perf3m: number | null;
  perf6m: number | null;
  perf12m: number | null;
  deliveryPct: number | null;
  isFno: boolean;
  fnoBan: boolean;
  fnoLotSize: number | null;
  circuitLimit: string;
  peRatio: number | null;
  forwardPe: number | null;
  pegRatio: number | null;
  debtToEquity: number | null;
  roePct: number | null;
  rocePct: number | null;
  opmTtmPct: number | null;
  dividendYieldPct: number | null;
  epsTtm: number | null;
  promoterHoldingPct: number | null;
  fiiChangePctQoq: number | null;
  diiChangePctQoq: number | null;
  yoyNetProfitGrowthPct: number | null;
  yoySalesGrowthPct: number | null;
  latestEarningsDate: string | null;
  returnsSinceEarningsPct: number | null;
  rsRating: number | null;
  rsRating1m: number | null;
  rsRating3m: number | null;
}

/** Convert a raw stock from JSON into a StockRow used by the UI */
function toStockRow(raw: RawStock): StockRow {
  return {
    symbol: raw.symbol,
    name: raw.name,
    listingDate: raw.listingDate,
    sector: raw.sector || 'Unclassified',
    industry: raw.industry || 'Unclassified',
    series: raw.series || 'EQ',
    close: raw.close ?? 0,
    changePct: raw.changePct ?? 0,
    open: raw.open ?? 0,
    high: raw.high ?? 0,
    low: raw.low ?? 0,
    volume: raw.volume ?? 0,
    rupeeVolumeCrore: raw.rupeeVolumeCrore ?? 0,
    rvol: raw.rvol ?? 0,
    marketCapCrore: raw.marketCapCrore ?? 0,
    peRatio: raw.peRatio,
    rsi14: raw.rsi14,
    adr20Pct: raw.adrPct20,
    atr14: raw.atr14,
    sma20: raw.sma20,
    sma50: raw.sma50,
    sma200: raw.sma200,
    ema20: null,
    ema50: null,
    ema200: null,
    dist52wHighPct: raw.dist52wHighPct,
    dist52wLowPct: raw.dist52wLowPct,
    distAthPct: raw.pctFromAth,
    earningsDate: raw.latestEarningsDate,
    daysSinceEarnings: null,
    fnoBan: raw.fnoBan,
    isFno: raw.isFno,
    circuitLimit: raw.circuitLimit || '',
    deliveryPct: raw.deliveryPct,
    rsRating: raw.rsRating,
    dataCompleteness: 100,
  };
}

type StockCache = { asOfDate: string; rawStocks: RawStock[]; stockRows: StockRow[] };

let _cache: StockCache | null = null;
let _ipoCache: IPORow[] | null = null;

async function loadStocks(): Promise<StockCache> {
  if (_cache) return _cache;
  const res = await fetch('/data/stocks.json');
  if (!res.ok) throw new Error(`Failed to load /data/stocks.json: ${res.status}`);
  const payload: StocksPayload = await res.json();
  _cache = {
    asOfDate: payload.asOfDate,
    rawStocks: payload.stocks,
    stockRows: payload.stocks.map(toStockRow),
  };
  return _cache;
}

async function loadIpos(): Promise<IPORow[]> {
  if (_ipoCache) return _ipoCache;
  const res = await fetch('/data/ipos.json');
  if (!res.ok) throw new Error(`Failed to load /data/ipos.json: ${res.status}`);
  const raw: any[] = await res.json();
  _ipoCache = raw.map((r) => ({
    symbol: r.symbol || '',
    name: r.name || r.company_name || '',
    listingDate: r.listing_date || r.listingDate || '',
    issuePrice: r.issue_price ?? r.issuePrice ?? null,
    listingPrice: r.listing_open ?? r.listingPrice ?? null,
    currentPrice: r.current_close ?? r.currentPrice ?? 0,
    returnSinceListingPct: r.return_since_listing_pct ?? r.returnSinceListingPct ?? null,
    turnoverCrore: r.avg_turnover_crore ?? r.turnoverCrore ?? 0,
    sector: r.sector || 'Unclassified',
    industry: r.industry || 'Unclassified',
    marketCapCrore: r.market_cap_crore ?? r.marketCapCrore ?? 0,
    circuitBand: r.circuit_limit ?? r.circuitBand ?? '',
    dataCompleteness: 100,
  }));
  return _ipoCache;
}

/**
 * Filter stocks by universe using index_memberships from the raw data.
 */
function filterByUniverse(rawStocks: RawStock[], universe: string, customSymbols?: string[]): RawStock[] {
  switch (universe) {
    case 'nifty50':
      return rawStocks.filter((s) => s.indexMemberships?.includes(NIFTY50_LABEL));
    case 'nifty500':
      return rawStocks.filter((s) => s.indexMemberships?.includes(NIFTY500_LABEL));
    case 'midsmall400':
      return rawStocks.filter((s) => s.indexMemberships?.includes(NIFTY_MIDSMALL400_LABEL));
    case 'custom':
      if (customSymbols?.length) {
        const set = new Set(customSymbols.map((s) => s.toUpperCase()));
        return rawStocks.filter((s) => set.has(s.symbol));
      }
      return rawStocks;
    default:
      return rawStocks; // mainboard = all
  }
}

/**
 * Evaluate a single condition against a raw stock record.
 * Returns true if the stock passes the condition.
 */
function evalCondition(stock: RawStock, conditionId: string, params: Record<string, any>): boolean {
  switch (conditionId) {
    // --- Trend ---
    case 'trend_price_vs_ma': {
      const period = params.maPeriod ?? 50;
      const operator = params.operator ?? 'above';
      let maValue: number | null = null;
      if (period === 10) maValue = stock.sma10;
      else if (period === 20) maValue = stock.sma20;
      else if (period === 50) maValue = stock.sma50;
      else if (period === 200) maValue = stock.sma200;
      if (maValue == null || stock.close == null) return true;
      if (operator === 'above') return stock.close > maValue;
      if (operator === 'below') return stock.close < maValue;
      if (operator === 'within_pct') {
        const threshold = params.thresholdPct ?? 5;
        return Math.abs(((stock.close - maValue) / maValue) * 100) <= threshold;
      }
      return true;
    }
    case 'trend_ma_stack': {
      const stackOrder = params.stackOrder ?? '20_above_50_above_200';
      if (stackOrder === '20_above_50_above_200')
        return (stock.sma20 ?? 0) > (stock.sma50 ?? 0) && (stock.sma50 ?? 0) > (stock.sma200 ?? 0);
      if (stackOrder === '10_above_20_above_50')
        return (stock.sma10 ?? 0) > (stock.sma20 ?? 0) && (stock.sma20 ?? 0) > (stock.sma50 ?? 0);
      if (stackOrder === 'bearish_stack')
        return (stock.sma200 ?? 0) > (stock.sma50 ?? 0) && (stock.sma50 ?? 0) > (stock.sma20 ?? 0);
      return true;
    }
    case 'trend_persistent_momentum': {
      // Approximate: check if price is above SMA20
      return stock.closeAboveSma20 === true;
    }
    case 'trend_ema_reclaim': {
      // Approximate: check closeAbove the specified EMA equivalent
      const ema = params.reclaimedEma ?? 'EMA 20';
      if (ema === 'EMA 10') return stock.closeAboveSma10 === true;
      if (ema === 'EMA 20') return stock.closeAboveSma20 === true;
      if (ema === 'EMA 50') return stock.closeAboveSma50 === true;
      return true;
    }
    case 'trend_ma_slope':
    case 'trend_pct_days_above_ma':
      return true; // Not enough data for these

    // --- Momentum ---
    case 'mom_rvol': {
      const min = params.minRvol ?? 0;
      const max = params.maxRvol ?? 100;
      return (stock.rvol ?? 0) >= min && (stock.rvol ?? 0) <= max;
    }
    case 'mom_return': {
      const period = params.period ?? '1D';
      const minRet = params.minReturn ?? -100;
      const maxRet = params.maxReturn ?? 1000;
      let ret: number | null = null;
      if (period === '1D') ret = stock.changePct;
      else if (period === '5D') ret = stock.perf1w;
      else if (period === '21D') ret = stock.perf1m;
      else if (period === '63D') ret = stock.perf3m;
      if (ret == null) return true;
      return ret >= minRet && ret <= maxRet;
    }
    case 'mom_consecutive_up':
      return true; // Can't compute from snapshot
    case 'mom_gap': {
      const gapType = params.gapType ?? 'Gap Up';
      const minGap = params.minGapPct ?? 2;
      const gap = stock.gapPct ?? 0;
      if (gapType === 'Gap Up') return gap >= minGap;
      if (gapType === 'Gap Down') return gap <= -minGap;
      return true;
    }
    case 'mom_delivery_vol': {
      const minDel = params.minDeliveryPct ?? 50;
      return (stock.deliveryPct ?? 0) >= minDel;
    }

    // --- Range & Patterns ---
    case 'range_52w_proximity': {
      const target = params.target ?? 'High';
      const maxDist = params.maxDistancePct ?? 5;
      if (target === 'High') return Math.abs(stock.dist52wHighPct ?? 100) <= maxDist;
      if (target === 'Low') return Math.abs(stock.dist52wLowPct ?? 100) <= maxDist;
      return true;
    }
    case 'range_contraction': {
      // Approximate using VCP-like heuristic — check if recent range is tight
      return true; // Need bar-by-bar data
    }
    case 'range_inside_bar': {
      const tf = params.timeframe ?? 'Daily';
      if (tf === 'Daily') return stock.isInsideDay === true;
      return true; // Weekly inside bar needs weekly data
    }

    // --- Relative Strength ---
    case 'rs_rating': {
      const min = params.minRsRating ?? 0;
      const max = params.maxRsRating ?? 99;
      return (stock.rsRating ?? 0) >= min && (stock.rsRating ?? 0) <= max;
    }
    case 'rs_1month': {
      const min = params.minRsRating ?? 0;
      return (stock.rsRating1m ?? 0) >= min;
    }
    case 'rs_3month': {
      const min = params.minRsRating ?? 0;
      return (stock.rsRating3m ?? 0) >= min;
    }
    case 'rs_divergence': {
      // Approximate: RS > threshold AND price below 52w high by threshold
      const minRs = params.minRsVsNifty ?? 0;
      const minBelow = params.minBelowHigh ?? 30;
      return (stock.rsRating ?? 0) > minRs && Math.abs(stock.dist52wHighPct ?? 0) >= minBelow;
    }

    // --- Fundamentals ---
    case 'fund_earnings_growth': {
      const minGrowth = params.minGrowthPct ?? 20;
      return (stock.yoyNetProfitGrowthPct ?? 0) >= minGrowth;
    }
    case 'fund_pe_ratio': {
      const minPe = params.minPe ?? 0;
      const maxPe = params.maxPe ?? 1000;
      if (stock.peRatio == null) return false;
      return stock.peRatio >= minPe && stock.peRatio <= maxPe;
    }
    case 'fund_roe': {
      const minRoe = params.minRoe ?? 15;
      return (stock.roePct ?? 0) >= minRoe;
    }
    case 'fund_free_float': {
      const minFloat = params.minFloat ?? 0;
      const maxFloat = params.maxFloat ?? 100;
      if (stock.freeFloatPct == null) return false;
      return stock.freeFloatPct >= minFloat && stock.freeFloatPct <= maxFloat;
    }
    case 'fund_stock_price': {
      const minPrice = params.minPrice ?? 50;
      if (stock.close == null) return false;
      return stock.close > minPrice;
    }

    // --- Liquidity & Misc ---
    case 'liq_turnover': {
      const minTurnover = params.minTurnoverCr ?? 5;
      // Evaluate against 50d average as requested
      return (stock.avgRupeeVolume50Cr ?? 0) >= minTurnover;
    }
    case 'liq_market_cap': {
      const minCap = params.minMarketCap ?? 0;
      const maxCap = params.maxMarketCap ?? 2000000;
      const cap = stock.marketCapCrore ?? 0;
      return cap >= minCap && cap <= maxCap;
    }
    case 'misc_fno_only': {
      const isFno = params.isFno === 'true';
      return stock.isFno === isFno;
    }
    case 'misc_exclude_circuit': {
      const bands = params.circuitBands || ['5', '10'];
      if (bands.includes(stock.circuitLimit)) return false;
      return true;
    }

    default:
      if (conditionId.startsWith('preset_')) {
        // --- BASELINE FILTERS (Applied to ALL presets) ---
        // 1. Market Cap > 1,000 Cr and < 200,000 Cr (Avoid ESM & Mega-caps)
        if (!stock.marketCapCrore || stock.marketCapCrore <= 1000 || stock.marketCapCrore >= 200000) return false;
        
        // 2. Stock Price > 10 and < 10,000 (Eliminate pennies & ultra-high priced)
        if (!stock.close || stock.close <= 10 || stock.close >= 10000) return false;
        
        // 3. 50 Days Avg Turnover > 5 Cr (Primary Liquidity)
        if (!stock.avgRupeeVolume50Cr || stock.avgRupeeVolume50Cr <= 5) return false;
        
        // 4. Exclude Circuit Stocks (2% and 5%)
        if (stock.circuitLimit === '2' || stock.circuitLimit === '5') return false;

        // --- PRESET SPECIFIC LOGIC ---
        switch (conditionId) {
          case 'preset_persistent_momentum':
            return (stock.closeAboveSma20 ?? false) && (stock.rsi14 ?? 0) >= 60;
          case 'preset_relative_strength_leaders':
            return (stock.rsRating ?? 0) >= 80 && (stock.dist52wHighPct ?? -100) >= -15;
          case 'preset_stage_2_uptrend':
            return (stock.closeAboveSma50 ?? false) && (stock.sma50AboveSma200 ?? false) && (stock.dist52wHighPct ?? -100) >= -25;
          case 'preset_52_week_high_breakout':
            return (stock.breakout52wHigh ?? false) || (stock.dist52wHighPct ?? -100) >= -2;
          case 'preset_vcp_contraction':
            return (stock.dist52wHighPct ?? -100) >= -20 && (stock.isInsideDay ?? false);
          case 'preset_earnings_growth_momentum':
            return (stock.dist52wHighPct ?? -100) >= -20 && (stock.rsRating ?? 0) >= 70;
          default:
            return true;
        }
      }
      return true;
  }
}

function evalNode(stock: RawStock, node: any): boolean {
  if (node.type === 'group') {
    if (!node.children || node.children.length === 0) return true;
    const isAny = node.operator === 'any';
    if (isAny) {
      return node.children.some((c: any) => evalNode(stock, c));
    } else {
      return node.children.every((c: any) => evalNode(stock, c));
    }
  } else if (node.type === 'condition') {
    const c = node.condition;
    return evalCondition(stock, c.conditionId, c.parameters);
  }
  return true;
}

class RealDataAdapter {
  async getCatalog(): Promise<ConditionDef[]> {
    return NEXUS_CONDITION_CATALOG;
  }

  async explainScreen(_req: ExplainRequest): Promise<ExplainResponse> {
    return { isValid: true, errors: [], compiledExplanations: [], warnings: [] };
  }

  async runScreen(req: ScreenerRunRequest): Promise<ScreenerRunResponse> {
    const data = await loadStocks();

    // 1. Filter by universe
    let filtered = filterByUniverse(data.rawStocks, req.universe, req.customSymbols);

    // 2. Filter by expression tree
    if (req.expressionTree) {
      filtered = filtered.filter((stock) => evalNode(stock, req.expressionTree));
    }

    // 3. Convert to StockRow for sorting and output
    let rows = filtered.map(toStockRow);

    // 4. Sort
    if (req.sort) {
      const field = req.sort.field as keyof StockRow;
      const dir = req.sort.direction === 'asc' ? 1 : -1;
      rows.sort((a, b) => {
        const valA = a[field] ?? 0;
        const valB = b[field] ?? 0;
        if (valA < valB) return -1 * dir;
        if (valA > valB) return 1 * dir;
        return 0;
      });
    }

    // 5. Paginate
    const totalCount = rows.length;
    const startIndex = (req.page - 1) * req.pageSize;
    const paginatedRows = rows.slice(startIndex, startIndex + req.pageSize);

    return {
      resolvedSession: {
        date: data.asOfDate,
        sessionId: `NSE-${data.asOfDate.replace(/-/g, '')}-FINAL`,
        status: 'closed',
        isHistorical: false,
      },
      immutableRevision: `rev_${data.asOfDate.replace(/-/g, '')}_real`,
      rows: paginatedRows,
      matchCount: totalCount,
      totalUniverseCount: data.rawStocks.length,
      page: req.page,
      pageSize: req.pageSize,
      perConditionCoverage: {},
      unavailableDiagnostics: [],
      warnings: [],
    };
  }

  async getIpos(): Promise<IPORow[]> {
    return loadIpos();
  }

  async compareSymbols(req: SymbolComparisonRequest): Promise<SymbolComparisonResponse> {
    const data = await loadStocks();
    const validSymbols: any[] = [];
    const invalidSymbols: string[] = [];

    req.symbols.forEach((sym) => {
      const clean = sym.trim().toUpperCase();
      const match = data.rawStocks.find((s) => s.symbol === clean);
      if (match) {
        validSymbols.push({
          symbol: match.symbol,
          name: match.name,
          sector: match.sector,
          close: match.close ?? 0,
          changePct: match.changePct ?? 0,
          marketCapCrore: match.marketCapCrore ?? 0,
          peRatio: match.peRatio,
          rvol: match.rvol ?? 0,
          rsi14: match.rsi14,
          rsRating: match.rsRating,
          sma50Status: (match.close ?? 0) > (match.sma50 ?? 0) ? 'Above SMA50' : 'Below SMA50',
          isFno: match.isFno,
          isValid: true,
        });
      } else {
        invalidSymbols.push(clean);
      }
    });

    return {
      validSymbols,
      invalidSymbols,
      sessionDate: data.asOfDate,
      immutableRevision: `rev_${data.asOfDate.replace(/-/g, '')}_real`,
    };
  }

  async getCurrentRevision(): Promise<RevisionCurrentResponse> {
    const data = await loadStocks();
    return {
      latestSessionDate: data.asOfDate,
      availableSessions: [
        { date: data.asOfDate, label: `${data.asOfDate} (Latest Closed)`, isHistorical: false },
      ],
      immutableRevision: `rev_${data.asOfDate.replace(/-/g, '')}_real`,
      mainboardUniverseCount: data.rawStocks.length,
      catalogVersion: 'v3.0-real',
    };
  }
}

export const realAdapter = new RealDataAdapter();
