import React, { useState } from 'react';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { screenerApi } from './api/screenerApi';
import { Navbar } from './components/Navbar';
import { ExploreTab } from './components/ExploreTab';
import { NewListingsTab } from './components/NewListingsTab';
import { BookmarkPlus } from 'lucide-react';
import { useLocalStorageState } from './hooks/useLocalStorageState';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 5 * 60 * 1000,
    },
  },
});

const ScreenerAppContent: React.FC = () => {
  const [activeTab, setActiveTab] = useLocalStorageState<'screener' | 'ipo'>('nexus-scanner.ui.active-tab.v1', 'screener');
  const [watchlistToast, setWatchlistToast] = useState<string | null>(null);

  const { data: revisionData, refetch: refreshRevision } = useQuery({
    queryKey: ['revisionCurrent'],
    queryFn: () => screenerApi.getCurrentRevision(),
    staleTime: 0,
    refetchInterval: 60_000,
    refetchOnWindowFocus: true,
  });

  const selectedAsOfDate = revisionData?.latestSessionDate || '';

  const handleAddToWatchlist = (symbols: string[]) => {
    setWatchlistToast(
      `Added ${symbols.length} symbol${symbols.length !== 1 ? 's' : ''} to watchlist`
    );
    setTimeout(() => setWatchlistToast(null), 2500);
  };

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <Navbar activeTab={activeTab} onTabChange={setActiveTab} />

      <main className="flex-1 max-w-screen-xl w-full mx-auto px-6 py-6">
        {activeTab === 'screener' ? (
          <ExploreTab
            selectedAsOfDate={selectedAsOfDate}
            datasetRevision={revisionData?.immutableRevision}
            onRefreshRevision={async () => (await refreshRevision()).data?.immutableRevision}
            onAddToWatchlist={handleAddToWatchlist}
          />
        ) : (
          <NewListingsTab key={revisionData?.immutableRevision} datasetRevision={revisionData?.immutableRevision} selectedAsOfDate={selectedAsOfDate} />
        )}
      </main>

      {watchlistToast && (
        <div className="fixed bottom-6 right-6 z-[9999] flex items-center gap-2 px-4 py-2.5 rounded-xl bg-gray-900 text-white text-xs font-medium shadow-xl animate-fade-in">
          <BookmarkPlus className="w-4 h-4 text-emerald-400" />
          <span>{watchlistToast}</span>
        </div>
      )}
    </div>
  );
};

export const App: React.FC = () => (
  <QueryClientProvider client={queryClient}>
    <ScreenerAppContent />
  </QueryClientProvider>
);

export default App;
