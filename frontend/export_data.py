#!/usr/bin/env python3
"""
Export real EDL pipeline data into static JSON files for the Nexus Screener frontend.

Reads:
  - all_stocks_fundamental_analysis.json.gz  (2,600 mainboard stocks)
  - rs_rating_daily.json.gz                  (RS percentile ratings)
  - nse_fno_ban.json.gz                      (current F&O ban list)
  - ipo_screener.json.gz                     (IPO records)

Writes:
  - frontend/public/data/stocks.json         (~1.8 MB, all screener fields)
  - frontend/public/data/ipos.json           (~150 KB, IPO catalogue)
"""

import gzip
import json
import os
import sys

PIPELINE_DIR = os.path.join(os.path.dirname(__file__), '..', 'DO NOT DELETE EDL PIPELINE')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'public', 'data')


def load_gz(filename):
    path = os.path.join(PIPELINE_DIR, filename)
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        return json.load(f)


def safe_round(val, decimals=2):
    if val is None:
        return None
    try:
        return round(float(val), decimals)
    except (TypeError, ValueError):
        return None


def main():
    print('Loading all_stocks_fundamental_analysis.json.gz ...')
    stocks_raw = load_gz('all_stocks_fundamental_analysis.json.gz')
    print(f'  → {len(stocks_raw)} stocks loaded')

    print('Loading rs_rating_daily.json.gz ...')
    rs_data = load_gz('rs_rating_daily.json.gz')
    # RS ratings are nested under the 'ratings' key
    rs_ratings = rs_data.get('ratings', {})
    print(f'  → {len(rs_ratings)} RS rating entries')

    print('Loading nse_fno_ban.json.gz ...')
    fno_ban_data = load_gz('nse_fno_ban.json.gz')
    fno_ban_symbols = set(fno_ban_data.get('symbols', []))
    print(f'  → {len(fno_ban_symbols)} stocks in F&O ban')

    # Build the frontend stocks array
    stocks_out = []
    as_of_date = None

    for s in stocks_raw:
        sym = s.get('symbol', '')
        if not sym:
            continue

        if not as_of_date:
            as_of_date = s.get('as_of_date', '2026-09-29')

        rs = rs_ratings.get(sym, {}) if rs_data.get('as_of_date') == s.get('as_of_date') else {}

        stock = {
            'symbol': sym,
            'name': s.get('name', ''),
            'listingDate': s.get('listing_date', ''),
            'sector': s.get('sector', 'Unclassified'),
            'industry': s.get('industry', ''),
            'series': s.get('listing_series', 'EQ'),
            'indexMemberships': s.get('index_memberships', []),

            # OHLCV
            'open': safe_round(s.get('open')),
            'high': safe_round(s.get('high')),
            'low': safe_round(s.get('low')),
            'close': safe_round(s.get('close')),
            'volume': s.get('volume'),
            'changePct': safe_round(s.get('change_percent')),
            'gapPct': safe_round(s.get('gap_percent')),

            # Market
            'marketCapCrore': safe_round(s.get('market_cap_crore'), 1),
            'rupeeVolumeCrore': safe_round((s.get('rupee_volume') or 0) / 1e7, 2),
            'avgRupeeVolume20Cr': safe_round(s.get('daily_rupee_turnover_20_cr'), 2),
            'avgRupeeVolume50Cr': safe_round(s.get('daily_rupee_turnover_50_cr'), 2),
            'sharesOutstanding': s.get('shares_outstanding'),
            'freeFloatPct': safe_round(s.get('free_float_percent')),

            # Technicals
            'sma10': safe_round(s.get('sma10')),
            'sma20': safe_round(s.get('sma20')),
            'sma50': safe_round(s.get('sma50')),
            'sma200': safe_round(s.get('sma200')),
            'rsi14': safe_round(s.get('rsi14'), 1),
            'atr14': safe_round(s.get('atr14')),
            'adrPct20': safe_round(s.get('adr_percent_20')),
            'rvol': safe_round(s.get('relative_volume_20'), 2),
            'pivotPoint': safe_round(s.get('pivot_point')),

            # MA relationships
            'closeAboveSma10': s.get('close_above_sma10', False),
            'closeAboveSma20': s.get('close_above_sma20', False),
            'closeAboveSma50': s.get('close_above_sma50', False),
            'closeAboveSma200': s.get('close_above_sma200', False),
            'sma10AboveSma20': s.get('sma10_above_sma20', False),
            'sma20AboveSma50': s.get('sma20_above_sma50', False),
            'sma50AboveSma200': s.get('sma50_above_sma200', False),
            'distFromSma20Pct': safe_round(s.get('distance_from_sma20_percent')),
            'distFromSma50Pct': safe_round(s.get('distance_from_sma50_percent')),
            'distFromSma200Pct': safe_round(s.get('distance_from_sma200_percent')),

            # 52-week
            'dist52wHighPct': safe_round(s.get('distance_from_52w_high_percent')),
            'dist52wLowPct': safe_round(s.get('distance_from_52w_low_percent')),
            'pctFromAth': safe_round(s.get('percent_from_ath')),

            # Pattern flags
            'bullishCandle': s.get('bullish_candle', False),
            'closeNearDayHigh': s.get('close_near_day_high', False),
            'breakout20dHigh': s.get('breakout_above_20d_high', False),
            'breakout50dHigh': s.get('breakout_above_50d_high', False),
            'near52wHigh': s.get('near_52w_high', False),
            'breakout52wHigh': s.get('breakout_above_52w_high', False),
            'isNr7': s.get('is_nr7', False),
            'isInsideDay': s.get('is_inside_day', False),
            'isBullishEngulfing': s.get('is_bullish_engulfing', False),

            # Performance returns
            'perf1w': safe_round(s.get('perf_1w')),
            'perf1m': safe_round(s.get('perf_1m')),
            'perf3m': safe_round(s.get('perf_3m')),
            'perf6m': safe_round(s.get('perf_6m')),
            'perf12m': safe_round(s.get('perf_12m')),

            # Delivery
            'deliveryPct': safe_round(s.get('delivery_percent'), 1),

            # F&O
            'isFno': s.get('fno_eligible', False),
            'fnoBan': sym in fno_ban_symbols if fno_ban_data.get('trade_date') == s.get('as_of_date') and fno_ban_data.get('available') else None,
            'fnoLotSize': s.get('fno_lot_size'),
            'circuitLimit': s.get('circuit_limit', ''),

            # Fundamentals
            'peRatio': safe_round(s.get('pe_ratio'), 1),
            'forwardPe': safe_round(s.get('forward_pe_ratio'), 1),
            'pegRatio': safe_round(s.get('peg_ratio'), 2),
            'debtToEquity': safe_round(s.get('debt_to_equity'), 2),
            'roePct': safe_round(s.get('roe_percent'), 1),
            'rocePct': safe_round(s.get('roce_percent'), 1),
            'opmTtmPct': safe_round(s.get('operating_margin_ttm_percent'), 1),
            'dividendYieldPct': safe_round(s.get('dividend_yield_percent'), 2),
            'epsTtm': safe_round(s.get('eps_ttm'), 2),
            'promoterHoldingPct': safe_round(s.get('promoter_holding_percent'), 1),
            'fiiChangePctQoq': safe_round(s.get('fii_percent_change_qoq'), 2),
            'diiChangePctQoq': safe_round(s.get('dii_percent_change_qoq'), 2),

            # Earnings
            'yoyNetProfitGrowthPct': safe_round(s.get('yoy_percent_net_profit_latest'), 1),
            'yoySalesGrowthPct': safe_round(s.get('yoy_percent_sales_latest'), 1),
            'latestEarningsDate': s.get('latest_earnings_date'),
            'returnsSinceEarningsPct': safe_round(s.get('returns_since_earnings_percent')),

            # RS Ratings (from separate file)
            'rsRating': safe_round(rs.get('front_weighted'), 1),
            'rsRating1m': safe_round(rs.get('one_month'), 1),
            'rsRating3m': safe_round(rs.get('three_month'), 1),
        }

        stocks_out.append(stock)

    # Sort by market cap descending for nicer default display
    stocks_out.sort(key=lambda x: x.get('marketCapCrore') or 0, reverse=True)

    # Build metadata wrapper
    output = {
        'asOfDate': as_of_date,
        'totalStocks': len(stocks_out),
        'stocks': stocks_out,
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stocks_path = os.path.join(OUTPUT_DIR, 'stocks.json')
    with open(stocks_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, separators=(',', ':'))
    size_mb = os.path.getsize(stocks_path) / (1024 * 1024)
    print(f'\n✓ Wrote {stocks_path}  ({len(stocks_out)} stocks, {size_mb:.2f} MB)')

    # --- IPO data ---
    print('\nLoading ipo_screener.json.gz ...')
    ipo_raw = load_gz('ipo_screener.json.gz')
    ipo_records = ipo_raw if isinstance(ipo_raw, list) else ipo_raw.get('records', [])
    print(f'  → {len(ipo_records)} IPO records loaded')

    ipo_path = os.path.join(OUTPUT_DIR, 'ipos.json')
    with open(ipo_path, 'w', encoding='utf-8') as f:
        json.dump(ipo_records, f, separators=(',', ':'))
    ipo_size_kb = os.path.getsize(ipo_path) / 1024
    print(f'✓ Wrote {ipo_path}  ({len(ipo_records)} records, {ipo_size_kb:.1f} KB)')

    print('\nDone! Frontend can now fetch /data/stocks.json and /data/ipos.json')


if __name__ == '__main__':
    main()
    from publish_snapshot import publish
    publish()
