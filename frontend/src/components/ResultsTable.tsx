import React, { useState } from 'react';
import { ScreenerRunResponse } from '../types/screener';
import {
  ArrowUpDown,
  Copy,
  Check,
  PlusCircle,
  AlertTriangle,
  FileQuestion,
  ChevronLeft,
  ChevronRight,
  TrendingUp,
  TrendingDown,
} from 'lucide-react';

interface ResultsTableProps {
  data?: ScreenerRunResponse;
  isLoading: boolean;
  isError: boolean;
  error?: Error | null;
  onPageChange: (page: number) => void;
  onSortChange: (field: string, direction: 'asc' | 'desc') => void;
  currentSort?: { field: string; direction: 'asc' | 'desc' };
  onAddToWatchlist?: (symbols: string[]) => void;
}

export const ResultsTable: React.FC<ResultsTableProps> = ({
  data,
  isLoading,
  isError,
  error,
  onPageChange,
  onSortChange,
  currentSort,
  onAddToWatchlist,
}) => {
  const [copiedTv, setCopiedTv] = useState(false);

  if (isLoading) {
    return (
      <div className="bg-white border border-slate-200/90 rounded-2xl p-8 text-center space-y-3 shadow-2xs">
        <div className="inline-block animate-spin rounded-full h-6 w-6 border-2 border-slate-900 border-t-transparent"></div>
        <p className="text-xs font-medium text-slate-600">Evaluating screener expression across NSE mainboard universe...</p>
      </div>
    );
  }

  if (isError) {
    return (
      <div className="bg-rose-50 border border-rose-200 rounded-2xl p-6 text-center space-y-2">
        <AlertTriangle className="h-7 w-7 text-rose-600 mx-auto" />
        <h4 className="text-sm font-semibold text-rose-900">Screener Query Failed</h4>
        <p className="text-xs text-rose-700 max-w-md mx-auto font-mono">
          {error?.message || 'An unexpected error occurred while executing the query.'}
        </p>
      </div>
    );
  }

  if (!data) return null;

  const handleCopyTradingView = () => {
    const tvList = data.rows.map((r) => `NSE:${r.symbol}`).join(', ');
    navigator.clipboard.writeText(tvList);
    setCopiedTv(true);
    setTimeout(() => setCopiedTv(false), 2000);
  };

  const handleSortClick = (field: string) => {
    const nextDir = currentSort?.field === field && currentSort.direction === 'asc' ? 'desc' : 'asc';
    onSortChange(field, nextDir);
  };

  const totalPages = Math.ceil(data.matchCount / data.pageSize);

  return (
    <div className="space-y-3">
      {/* Crisp Header Bar */}
      <div className="flex flex-wrap items-center justify-between gap-3 bg-white border border-slate-200/90 px-4 py-3 rounded-2xl shadow-2xs">
        <div className="flex items-center space-x-3">
          <div className="text-xs">
            <span className="text-slate-500 font-medium">Matched Equities: </span>
            <strong className="text-slate-900 font-mono text-sm">{data.matchCount}</strong>
            <span className="text-slate-400 text-[11px] ml-1">/ {data.totalUniverseCount} Universe</span>
          </div>

          {/* Diagnostics warning */}
          {data.unavailableDiagnostics.length > 0 && (
            <div className="flex items-center space-x-1.5 px-2 py-0.5 rounded-md bg-amber-50 border border-amber-200 text-amber-800 text-[11px]">
              <AlertTriangle className="h-3 w-3 text-amber-600" />
              <span>Partial metrics unavailable for date</span>
            </div>
          )}
        </div>

        {/* Toolbar Actions */}
        <div className="flex items-center space-x-2">
          <button
            onClick={handleCopyTradingView}
            disabled={data.rows.length === 0}
            className="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-white hover:bg-slate-50 text-slate-700 text-xs font-medium border border-slate-200 transition-colors disabled:opacity-40 cursor-pointer shadow-2xs"
          >
            {copiedTv ? <Check className="h-3.5 w-3.5 text-emerald-600" /> : <Copy className="h-3.5 w-3.5 text-slate-500" />}
            <span>{copiedTv ? 'Copied!' : 'Copy TradingView'}</span>
          </button>

          {onAddToWatchlist && (
            <button
              onClick={() => onAddToWatchlist(data.rows.map((r) => r.symbol))}
              disabled={data.rows.length === 0}
              className="flex items-center space-x-1.5 px-3 py-1.5 rounded-lg bg-white hover:bg-slate-50 text-slate-700 text-xs font-medium border border-slate-200 transition-colors disabled:opacity-40 cursor-pointer shadow-2xs"
            >
              <PlusCircle className="h-3.5 w-3.5 text-emerald-600" />
              <span>Watchlist</span>
            </button>
          )}
        </div>
      </div>

      {/* Main Results Table */}
      <div className="bg-white border border-slate-200/90 rounded-2xl overflow-hidden shadow-2xs">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs text-slate-800">
            <thead className="bg-slate-50/80 text-slate-500 uppercase text-[10px] tracking-wider border-b border-slate-200/90">
              <tr>
                <th className="py-3 px-4 font-semibold">Symbol & Name</th>
                <th className="py-3 px-4 font-semibold">Sector / Industry</th>
                <th
                  onClick={() => handleSortClick('close')}
                  className="py-3 px-4 font-semibold cursor-pointer hover:text-slate-900 transition-colors"
                >
                  <div className="flex items-center space-x-1">
                    <span>Close Price</span>
                    <ArrowUpDown className="h-3 w-3" />
                  </div>
                </th>
                <th
                  onClick={() => handleSortClick('changePct')}
                  className="py-3 px-4 font-semibold cursor-pointer hover:text-slate-900 transition-colors"
                >
                  <div className="flex items-center space-x-1">
                    <span>1D Return</span>
                    <ArrowUpDown className="h-3 w-3" />
                  </div>
                </th>
                <th
                  onClick={() => handleSortClick('rvol')}
                  className="py-3 px-4 font-semibold cursor-pointer hover:text-slate-900 transition-colors"
                >
                  <div className="flex items-center space-x-1">
                    <span>20D RVOL</span>
                    <ArrowUpDown className="h-3 w-3" />
                  </div>
                </th>
                <th
                  onClick={() => handleSortClick('marketCapCrore')}
                  className="py-3 px-4 font-semibold cursor-pointer hover:text-slate-900 transition-colors"
                >
                  <div className="flex items-center space-x-1">
                    <span>Market Cap (Cr)</span>
                    <ArrowUpDown className="h-3 w-3" />
                  </div>
                </th>
                <th className="py-3 px-4 font-semibold">RS & RSI</th>
                <th className="py-3 px-4 font-semibold">Delivery %</th>
                <th className="py-3 px-4 font-semibold">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 font-mono">
              {data.rows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="py-10 text-center text-slate-500 font-sans">
                    <FileQuestion className="h-7 w-7 text-slate-400 mx-auto mb-2" />
                    <p className="text-xs font-semibold text-slate-700">No Equities Matched Screener Criteria</p>
                    <p className="text-[11px] text-slate-400 mt-0.5">
                      Try adjusting threshold parameters or choosing a broader universe
                    </p>
                  </td>
                </tr>
              ) : (
                data.rows.map((row) => (
                  <tr key={row.symbol} className="hover:bg-slate-50/70 transition-colors">
                    {/* Symbol & Name */}
                    <td className="py-3.5 px-4 font-sans">
                      <div className="font-bold text-xs text-slate-900 flex items-center space-x-1.5">
                        <span>{row.symbol}</span>
                        {row.isFno && (
                          <span className="text-[9px] font-semibold px-1 py-0.2 rounded bg-indigo-50 text-indigo-700 border border-indigo-200">
                            F&O
                          </span>
                        )}
                      </div>
                      <div className="text-[11px] text-slate-500 truncate max-w-[170px]">{row.name}</div>
                    </td>

                    {/* Sector & Industry */}
                    <td className="py-3.5 px-4 font-sans">
                      <div
                        className={`text-xs ${
                          row.sector === 'Unclassified'
                            ? 'italic text-slate-400 font-normal'
                            : 'font-medium text-slate-700'
                        }`}
                      >
                        {row.sector}
                      </div>
                      <div className="text-[10px] text-slate-400 truncate max-w-[140px]">{row.industry}</div>
                    </td>

                    {/* Close */}
                    <td className="py-3.5 px-4 font-semibold text-slate-900">
                      ₹{row.close.toLocaleString('en-IN', { minimumFractionDigits: 2 })}
                    </td>

                    {/* 1D Return */}
                    <td className="py-3.5 px-4 font-semibold">
                      <div
                        className={`flex items-center space-x-1 ${
                          row.changePct >= 0 ? 'text-emerald-600' : 'text-rose-600'
                        }`}
                      >
                        {row.changePct >= 0 ? (
                          <TrendingUp className="h-3.5 w-3.5" />
                        ) : (
                          <TrendingDown className="h-3.5 w-3.5" />
                        )}
                        <span>{row.changePct >= 0 ? `+${row.changePct}%` : `${row.changePct}%`}</span>
                      </div>
                    </td>

                    {/* RVOL */}
                    <td className="py-3.5 px-4">
                      <span
                        className={`px-2 py-0.5 rounded text-[11px] font-semibold ${
                          row.rvol >= 2.0
                            ? 'bg-emerald-50 text-emerald-700 border border-emerald-200'
                            : row.rvol >= 1.2
                            ? 'bg-blue-50 text-blue-700 border border-blue-200'
                            : 'text-slate-600'
                        }`}
                      >
                        {row.rvol}x
                      </span>
                    </td>

                    {/* Market Cap */}
                    <td className="py-3.5 px-4 text-slate-700">
                      ₹{Math.round(row.marketCapCrore).toLocaleString('en-IN')} Cr
                    </td>

                    {/* Technicals */}
                    <td className="py-3.5 px-4 font-sans text-xs space-y-0.5">
                      <div className="flex items-center space-x-1.5">
                        <span className="text-slate-400 text-[10px]">RS:</span>
                        <span className="font-mono font-bold text-slate-900 text-xs">{row.rsRating || 'N/A'}</span>
                      </div>
                      <div className="text-[10px] text-slate-400">
                        RSI: <span className="text-slate-700 font-mono">{row.rsi14 || 'N/A'}</span>
                      </div>
                    </td>

                    {/* Delivery % */}
                    <td className="py-3.5 px-4 font-mono">
                      {row.deliveryPct === null ? (
                        <span className="text-[10px] italic text-slate-400 font-sans">Unavailable</span>
                      ) : (
                        <span className="font-semibold text-slate-700">{row.deliveryPct}%</span>
                      )}
                    </td>

                    {/* Status / Ban */}
                    <td className="py-3.5 px-4 font-sans">
                      {row.fnoBan ? (
                        <span className="px-2 py-0.5 text-[10px] font-semibold rounded bg-rose-50 text-rose-700 border border-rose-200">
                          F&O BAN
                        </span>
                      ) : (
                        <span className="px-2 py-0.5 text-[10px] font-medium rounded bg-slate-100 text-slate-600 border border-slate-200">
                          Normal
                        </span>
                      )}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination Controls */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 bg-slate-50/80 border-t border-slate-200/90 text-xs">
            <span className="text-slate-500">
              Page <strong className="text-slate-900 font-mono">{data.page}</strong> of{' '}
              <strong className="text-slate-900 font-mono">{totalPages}</strong>
            </span>
            <div className="flex items-center space-x-1.5">
              <button
                onClick={() => onPageChange(data.page - 1)}
                disabled={data.page === 1}
                className="p-1.5 rounded-lg bg-white border border-slate-200 text-slate-700 disabled:opacity-40 hover:bg-slate-50 transition-colors cursor-pointer"
              >
                <ChevronLeft className="h-3.5 w-3.5" />
              </button>
              <button
                onClick={() => onPageChange(data.page + 1)}
                disabled={data.page >= totalPages}
                className="p-1.5 rounded-lg bg-white border border-slate-200 text-slate-700 disabled:opacity-40 hover:bg-slate-50 transition-colors cursor-pointer"
              >
                <ChevronRight className="h-3.5 w-3.5" />
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
