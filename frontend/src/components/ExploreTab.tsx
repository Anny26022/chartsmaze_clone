import React, { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  UniverseType,
  MatchMode,
  ActiveCondition,
  ExpressionNode,
  ScreenerRunRequest,
} from '../types/screener';
import { screenerApi } from '../api/screenerApi';
import { NEXUS_CONDITION_CATALOG } from '../data/conditionCatalog';
import { PRESET_CATALOG } from '../data/presetCatalog';
import { explainExpressionTree } from '../utils/nqlParser';
import { ResultsTable } from './ResultsTable';
import { ScreenerModal } from './ScreenerModal';
import { SlidersHorizontal, Plus, X, RotateCcw, Play, ChevronDown } from 'lucide-react';

interface ExploreTabProps {
  selectedAsOfDate: string;
  onAddToWatchlist?: (symbols: string[]) => void;
}

const DEFAULT_CONDITIONS: Record<string, ActiveCondition> = {
  mom_rvol: {
    instanceId: 'default_rvol',
    conditionId: 'mom_rvol',
    parameters: { minRvol: 1.5, maxRvol: 20.0 },
  },
  trend_price_vs_ma: {
    instanceId: 'default_sma50',
    conditionId: 'trend_price_vs_ma',
    parameters: { maType: 'SMA', maPeriod: 50, operator: 'above', thresholdPct: 0 },
  },
};

export const ExploreTab: React.FC<ExploreTabProps> = ({ selectedAsOfDate, onAddToWatchlist }) => {
  const [universe, setUniverse] = useState<UniverseType>('mainboard');
  const [matchMode, setMatchMode] = useState<MatchMode>('all');
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [activeConditionsMap, setActiveConditionsMap] =
    useState<Record<string, ActiveCondition>>(DEFAULT_CONDITIONS);
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState<{ field: string; direction: 'asc' | 'desc' }>({
    field: 'rvol',
    direction: 'desc',
  });

  const activeConditionsArray = useMemo(() => Object.values(activeConditionsMap), [activeConditionsMap]);

  const expressionTree: ExpressionNode = useMemo(
    () => ({
      type: 'group',
      operator: matchMode,
      children: activeConditionsArray.map((c) => ({ type: 'condition', condition: c })),
    }),
    [matchMode, activeConditionsArray]
  );

  const explanationResult = useMemo(
    () => explainExpressionTree(expressionTree, selectedAsOfDate),
    [expressionTree, selectedAsOfDate]
  );

  const runRequest: ScreenerRunRequest = useMemo(
    () => ({
      expressionTree,
      universe,
      asOfDate: selectedAsOfDate,
      sort,
      page,
      pageSize: 15,
    }),
    [expressionTree, universe, selectedAsOfDate, sort, page]
  );

  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['screenRun', runRequest],
    queryFn: () => screenerApi.runScreen(runRequest),
  });

  const handleApply = (newMap: Record<string, ActiveCondition>, newMatchMode: MatchMode) => {
    setActiveConditionsMap(newMap);
    setMatchMode(newMatchMode);
    setPage(1);
  };

  const handleRemove = (id: string) => {
    setActiveConditionsMap((prev) => {
      const next = { ...prev };
      delete next[id];
      return next;
    });
    setPage(1);
  };

  const handleReset = () => {
    setActiveConditionsMap({});
    setPage(1);
  };

  const filterCount = activeConditionsArray.length;

  return (
    <div className="space-y-4">
      {/* ── Top Control Bar ── */}
      <div className="bg-white rounded-2xl border border-gray-200 p-4 shadow-sm">
        <div className="flex flex-wrap items-center justify-between gap-3">

          {/* Left: Universe Dropdown */}
          <div className="flex items-center gap-3">
            <div className="relative">
              <select
                value={universe}
                onChange={(e) => { setUniverse(e.target.value as UniverseType); setPage(1); }}
                className="appearance-none bg-gray-50 border border-gray-200 text-gray-800 text-xs font-semibold rounded-xl pl-3 pr-8 py-2 focus:outline-none focus:border-gray-400 cursor-pointer"
              >
                <option value="mainboard">Mainboard · 2,240 stocks</option>
                <option value="nifty50">Nifty 50</option>
                <option value="nifty500">Nifty 500</option>
                <option value="midsmall400">Nifty MidSmall 400</option>
              </select>
              <ChevronDown className="absolute right-2.5 top-2.5 w-3.5 h-3.5 text-gray-400 pointer-events-none" />
            </div>

            {/* Match Mode Toggle */}
            <div className="flex items-center bg-gray-100 rounded-xl p-0.5 text-xs">
              {(['all', 'any'] as MatchMode[]).map((m) => (
                <button
                  key={m}
                  onClick={() => setMatchMode(m)}
                  className={`px-3 py-1.5 rounded-lg font-medium transition-all cursor-pointer ${
                    matchMode === m
                      ? 'bg-white text-gray-900 shadow-sm font-semibold'
                      : 'text-gray-500 hover:text-gray-700'
                  }`}
                >
                  {m === 'all' ? 'Match ALL' : 'Match ANY'}
                </button>
              ))}
            </div>
          </div>

          {/* Right: Actions */}
          <div className="flex items-center gap-2">
            {/* Edit Filters Button */}
            <button
              onClick={() => setIsModalOpen(true)}
              className="flex items-center gap-2 px-4 py-2 bg-gray-900 hover:bg-gray-800 text-white text-xs font-semibold rounded-xl transition-colors cursor-pointer shadow-sm"
            >
              <SlidersHorizontal className="w-3.5 h-3.5" />
              <span>Edit Filters</span>
              {filterCount > 0 && (
                <span className="bg-emerald-500 text-white text-[10px] font-bold rounded-full w-4 h-4 flex items-center justify-center leading-none">
                  {filterCount}
                </span>
              )}
            </button>

            {/* Run Screen Button */}
            <button
              onClick={() => refetch()}
              className="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-500 text-white text-xs font-semibold rounded-xl transition-colors cursor-pointer shadow-sm"
            >
              <Play className="w-3.5 h-3.5 fill-current" />
              <span>Run Screen</span>
            </button>
          </div>
        </div>

        {/* ── Active Filter Pills ── */}
        <div className="flex flex-wrap items-center gap-2 mt-3 pt-3 border-t border-gray-100">
          <span className="text-[11px] font-semibold text-gray-400 uppercase tracking-wide">
            Active Filters
          </span>

          {filterCount === 0 ? (
            <span className="text-xs text-gray-400 italic">None — showing full universe</span>
          ) : (
            activeConditionsArray.map((cond) => {
              const def = NEXUS_CONDITION_CATALOG.find((c) => c.id === cond.conditionId) || 
                          PRESET_CATALOG.find((c) => c.id === cond.conditionId);
              return (
                <span
                  key={cond.conditionId}
                  className="inline-flex items-center gap-1.5 px-2.5 py-1 bg-emerald-50 text-emerald-800 border border-emerald-200 rounded-lg text-xs font-medium"
                >
                  {def?.label ?? cond.conditionId}
                  <button
                    onClick={() => handleRemove(cond.conditionId)}
                    className="text-emerald-600 hover:text-emerald-900 cursor-pointer"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </span>
              );
            })
          )}

          <button
            onClick={() => setIsModalOpen(true)}
            className="inline-flex items-center gap-1 px-2.5 py-1 border border-dashed border-gray-300 text-gray-500 hover:text-gray-700 hover:border-gray-400 rounded-lg text-xs font-medium transition-colors cursor-pointer"
          >
            <Plus className="w-3 h-3" />
            Add
          </button>

          {filterCount > 0 && (
            <button
              onClick={handleReset}
              className="inline-flex items-center gap-1 text-xs text-gray-400 hover:text-gray-600 cursor-pointer ml-auto"
            >
              <RotateCcw className="w-3 h-3" />
              Reset all
            </button>
          )}
        </div>

        {/* ── Query Explanation ── */}
        {explanationResult.compiledExplanations.length > 0 && (
          <div className="mt-2 px-3 py-2 bg-gray-50 rounded-xl border border-gray-100 text-[11px] text-gray-600 font-mono truncate">
            {explanationResult.compiledExplanations
              .map((e) => e.humanReadableText)
              .join(matchMode === 'all' ? ' AND ' : ' OR ')}
          </div>
        )}
      </div>

      {/* ── Results Table ── */}
      <ResultsTable
        data={data}
        isLoading={isLoading}
        isError={isError}
        error={error}
        onPageChange={setPage}
        onSortChange={(field, direction) => setSort({ field, direction })}
        currentSort={sort}
        onAddToWatchlist={onAddToWatchlist}
      />

      {/* ── Screener Modal ── */}
      <ScreenerModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        activeConditionsMap={activeConditionsMap}
        matchMode={matchMode}
        onApply={handleApply}
      />
    </div>
  );
};
