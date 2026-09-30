import React, { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { screenerApi } from '../api/screenerApi';
import { Layers, CheckCircle2, XCircle, Play, PlusCircle, RefreshCw } from 'lucide-react';

interface SymbolListTabProps {
  onRunOnCustomSymbols: (symbols: string[]) => void;
  onAddToWatchlist?: (symbols: string[]) => void;
}

export const SymbolListTab: React.FC<SymbolListTabProps> = ({
  onRunOnCustomSymbols,
  onAddToWatchlist,
}) => {
  const [inputText, setInputText] = useState(
    'RELIANCE, TCS, HDFCBANK, INVALID_TICKER_XYZ, DIXON, SUZLON\nMAZDOCK, KAYNES'
  );

  // Symbol Comparison Mutation via React Query
  const compareMutation = useMutation({
    mutationFn: (symbols: string[]) => screenerApi.compareSymbols({ symbols }),
  });

  // Handle parsing and normalizing symbols on submission
  const handleParseAndCompare = () => {
    const rawList = inputText
      .split(/[\s,;\n]+/)
      .map((s) => s.trim().toUpperCase())
      .filter((s) => s.length > 0);

    const uniqueSymbols = Array.from(new Set(rawList));
    if (uniqueSymbols.length > 0) {
      compareMutation.mutate(uniqueSymbols);
    }
  };

  const comparisonData = compareMutation.data;

  return (
    <div className="space-y-4 animate-fade-in">
      {/* Header Banner */}
      <div className="bg-white border border-slate-200/80 p-4 sm:p-5 rounded-2xl shadow-xs flex items-center justify-between">
        <div className="flex items-center space-x-3.5">
          <div className="p-2 bg-indigo-50 text-indigo-700 rounded-lg border border-indigo-200">
            <Layers className="h-5 w-5" />
          </div>
          <div>
            <h2 className="text-sm font-bold text-slate-900">Custom Symbol Batch Normalizer</h2>
            <p className="text-xs text-slate-500">
              Paste arbitrary comma-, space-, or newline-separated NSE tickers to normalize & validate
            </p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Input Area */}
        <div className="bg-white border border-slate-200/80 p-4 sm:p-5 rounded-2xl shadow-xs space-y-3 flex flex-col">
          <label className="block text-xs font-semibold text-slate-700">
            Paste Ticker Symbols
          </label>
          <textarea
            rows={7}
            value={inputText}
            onChange={(e) => setInputText(e.target.value)}
            placeholder="Paste symbols e.g.: RELIANCE, TCS, HDFCBANK, TATAMOTORS, DIXON..."
            className="w-full bg-slate-50 border border-slate-200 rounded-xl p-3 text-xs font-mono text-slate-900 placeholder-slate-400 focus:outline-none focus:border-slate-400 flex-1"
          />

          <div className="flex justify-end pt-1">
            <button
              onClick={handleParseAndCompare}
              disabled={compareMutation.isPending || !inputText.trim()}
              className="flex items-center space-x-1.5 px-4 py-2 rounded-lg bg-slate-900 hover:bg-slate-800 text-white text-xs font-semibold shadow-xs transition-all disabled:opacity-40 cursor-pointer"
            >
              {compareMutation.isPending ? (
                <RefreshCw className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <CheckCircle2 className="h-3.5 w-3.5" />
              )}
              <span>Normalize & Validate Batch</span>
            </button>
          </div>
        </div>

        {/* Results & Verification Panel */}
        <div className="bg-white border border-slate-200/80 p-4 sm:p-5 rounded-2xl shadow-xs space-y-3 flex flex-col">
          <h3 className="text-xs font-semibold text-slate-700">Symbol Normalization Diagnostics</h3>

          {!comparisonData ? (
            <div className="p-8 text-center border border-dashed border-slate-200 rounded-xl space-y-2 text-slate-400 my-auto">
              <Layers className="h-7 w-7 mx-auto text-slate-300" />
              <p className="text-xs font-medium text-slate-600">No symbols parsed yet</p>
              <p className="text-[11px] text-slate-400">Click "Normalize & Validate Batch" to verify ticker list</p>
            </div>
          ) : (
            <div className="space-y-4 flex-1 flex flex-col justify-between">
              {/* Summary Stats */}
              <div className="grid grid-cols-2 gap-3">
                <div className="p-3 bg-emerald-50 border border-emerald-200 rounded-xl">
                  <div className="flex items-center space-x-1.5 text-emerald-800 font-semibold text-xs">
                    <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600" />
                    <span>Valid Equities ({comparisonData.validSymbols.length})</span>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-1">
                    {comparisonData.validSymbols.map((s) => (
                      <span
                        key={s.symbol}
                        className="px-1.5 py-0.5 rounded bg-white text-emerald-800 font-mono text-[10px] border border-emerald-200"
                      >
                        {s.symbol}
                      </span>
                    ))}
                  </div>
                </div>

                <div className="p-3 bg-rose-50 border border-rose-200 rounded-xl">
                  <div className="flex items-center space-x-1.5 text-rose-800 font-semibold text-xs">
                    <XCircle className="h-3.5 w-3.5 text-rose-600" />
                    <span>Invalid / Unavailable ({comparisonData.invalidSymbols.length})</span>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-1">
                    {comparisonData.invalidSymbols.length === 0 ? (
                      <span className="text-[11px] text-slate-400 italic">None</span>
                    ) : (
                      comparisonData.invalidSymbols.map((sym) => (
                        <span
                          key={sym}
                          className="px-1.5 py-0.5 rounded bg-white text-rose-800 font-mono text-[10px] border border-rose-200"
                        >
                          {sym}
                        </span>
                      ))
                    )}
                  </div>
                </div>
              </div>

              {/* Action Buttons */}
              <div className="flex flex-col sm:flex-row gap-2.5 pt-2">
                <button
                  onClick={() =>
                    onRunOnCustomSymbols(comparisonData.validSymbols.map((s) => s.symbol))
                  }
                  disabled={comparisonData.validSymbols.length === 0}
                  className="flex-1 flex items-center justify-center space-x-1.5 px-4 py-2 rounded-lg bg-slate-900 hover:bg-slate-800 text-white font-semibold text-xs shadow-xs transition-all disabled:opacity-40 cursor-pointer"
                >
                  <Play className="h-3.5 w-3.5 fill-current" />
                  <span>Run Screener on Valid List</span>
                </button>

                {onAddToWatchlist && (
                  <button
                    onClick={() =>
                      onAddToWatchlist(comparisonData.validSymbols.map((s) => s.symbol))
                    }
                    disabled={comparisonData.validSymbols.length === 0}
                    className="flex items-center justify-center space-x-1.5 px-4 py-2 rounded-lg bg-white hover:bg-slate-50 text-slate-700 font-semibold text-xs border border-slate-200 transition-all disabled:opacity-40 cursor-pointer shadow-2xs"
                  >
                    <PlusCircle className="h-3.5 w-3.5 text-emerald-600" />
                    <span>Watchlist</span>
                  </button>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
