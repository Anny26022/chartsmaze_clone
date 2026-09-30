import React, { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { screenerApi } from '../api/screenerApi';
import { Calendar, TrendingUp, TrendingDown, ShieldCheck } from 'lucide-react';

export const NewListingsTab: React.FC = () => {
  const [periodFilter, setPeriodFilter] = useState<'30d' | '90d' | '6m' | '1y' | 'all'>('all');

  const { data: ipoData = [], isLoading } = useQuery({
    queryKey: ['ipos'],
    queryFn: () => screenerApi.getIpos(),
  });

  return (
    <div className="space-y-3 animate-fade-in">
      {/* Clean Single Control Line (No Heavy Banner Box!) */}
      <div className="flex flex-wrap items-center justify-between gap-3 px-1 py-1">
        <h2 className="text-sm font-bold text-slate-900 tracking-tight">
          NSE New Listings & IPO Catalogue
        </h2>

        {/* Period Filter Pills */}
        <div className="flex items-center space-x-1 bg-slate-100 p-0.5 rounded-lg border border-slate-200 text-xs">
          <span className="text-slate-500 font-medium px-2 text-[11px]">Listing Window:</span>
          {(['30d', '90d', '6m', '1y', 'all'] as const).map((p) => (
            <button
              key={p}
              onClick={() => setPeriodFilter(p)}
              className={`px-2.5 py-1 rounded font-medium text-xs uppercase transition-all cursor-pointer ${
                periodFilter === p
                  ? 'bg-white text-slate-900 shadow-2xs border border-slate-200 font-semibold'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              {p === '30d' ? '30 Days' : p === '90d' ? '90 Days' : p === '6m' ? '6 Months' : p === '1y' ? '1 Year' : 'All'}
            </button>
          ))}
        </div>
      </div>

      {/* Clean IPO Data Table */}
      <div className="bg-white border border-slate-200/90 rounded-2xl overflow-hidden shadow-2xs">
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs text-slate-800">
            <thead className="bg-slate-50/80 text-slate-500 uppercase text-[10px] tracking-wider border-b border-slate-200/90">
              <tr>
                <th className="py-3 px-4 font-semibold">Symbol & Company</th>
                <th className="py-3 px-4 font-semibold">Listing Date</th>
                <th className="py-3 px-4 font-semibold">Issue Price</th>
                <th className="py-3 px-4 font-semibold">Listing Price</th>
                <th className="py-3 px-4 font-semibold">Current Price</th>
                <th className="py-3 px-4 font-semibold">Return Since Listing</th>
                <th className="py-3 px-4 font-semibold">Daily Turnover</th>
                <th className="py-3 px-4 font-semibold">Sector / Industry</th>
                <th className="py-3 px-4 font-semibold">Circuit Band</th>
                <th className="py-3 px-4 font-semibold">Completeness</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100 font-mono">
              {isLoading ? (
                <tr>
                  <td colSpan={10} className="py-10 text-center text-slate-500 font-sans">
                    <div className="inline-block animate-spin rounded-full h-5 w-5 border-2 border-slate-900 border-t-transparent mb-2"></div>
                    <p className="text-xs font-medium">Loading IPO catalogue...</p>
                  </td>
                </tr>
              ) : ipoData.length === 0 ? (
                <tr>
                  <td colSpan={10} className="py-10 text-center text-slate-400 font-sans">
                    No new listings recorded for selected period window.
                  </td>
                </tr>
              ) : (
                ipoData.map((row) => {
                  const ret = row.returnSinceListingPct;
                  return (
                    <tr key={row.symbol} className="hover:bg-slate-50/70 transition-colors">
                      {/* Symbol & Name */}
                      <td className="py-3.5 px-4 font-sans">
                        <div className="font-bold text-xs text-slate-900">{row.symbol}</div>
                        <div className="text-[11px] text-slate-500 truncate max-w-[150px]">{row.name}</div>
                      </td>

                      {/* Listing Date */}
                      <td className="py-3.5 px-4 text-slate-700">
                        <div className="flex items-center space-x-1.5">
                          <Calendar className="h-3 w-3 text-slate-400" />
                          <span>{row.listingDate}</span>
                        </div>
                      </td>

                      {/* Issue Price */}
                      <td className="py-3.5 px-4 text-slate-700">
                        {row.issuePrice ? `₹${row.issuePrice}` : <span className="text-slate-400 text-[10px] font-sans">Unspecified</span>}
                      </td>

                      {/* Listing Price */}
                      <td className="py-3.5 px-4 text-slate-700">
                        {row.listingPrice ? `₹${row.listingPrice}` : <span className="text-slate-400 text-[10px] font-sans">Unspecified</span>}
                      </td>

                      {/* Current Price */}
                      <td className="py-3.5 px-4 font-semibold text-slate-900">
                        ₹{row.currentPrice.toLocaleString('en-IN', { minimumFractionDigits: 2 })}
                      </td>

                      {/* Return Since Listing */}
                      <td className="py-3.5 px-4 font-semibold">
                        {ret === null ? (
                          <span className="text-slate-400 text-[10px] font-sans">N/A</span>
                        ) : (
                          <div className={`flex items-center space-x-1 ${ret >= 0 ? 'text-emerald-600' : 'text-rose-600'}`}>
                            {ret >= 0 ? <TrendingUp className="h-3.5 w-3.5" /> : <TrendingDown className="h-3.5 w-3.5" />}
                            <span>{ret >= 0 ? `+${ret}%` : `${ret}%`}</span>
                          </div>
                        )}
                      </td>

                      {/* Turnover */}
                      <td className="py-3.5 px-4 text-slate-700">
                        ₹{row.turnoverCrore.toLocaleString('en-IN')} Cr
                      </td>

                      {/* Sector */}
                      <td className="py-3.5 px-4 font-sans text-xs">
                        <div className="text-slate-800 font-medium">{row.sector}</div>
                        <div className="text-[10px] text-slate-400">{row.industry}</div>
                      </td>

                      {/* Circuit Band */}
                      <td className="py-3.5 px-4 font-sans">
                        <span className="px-2 py-0.5 text-[10px] font-medium rounded bg-slate-100 text-slate-600 border border-slate-200">
                          {row.circuitBand}
                        </span>
                      </td>

                      {/* Completeness Badge */}
                      <td className="py-3.5 px-4 font-sans">
                        <span className="flex items-center space-x-1 text-xs font-semibold text-slate-900">
                          <ShieldCheck className="h-3.5 w-3.5 text-emerald-600" />
                          <span>{row.dataCompleteness}%</span>
                        </span>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
