import React, { useState } from 'react';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { screenerApi } from './api/screenerApi';
import { Navbar } from './components/Navbar';
import { ExploreTab } from './components/ExploreTab';
import { NewListingsTab } from './components/NewListingsTab';
import { BookmarkPlus } from 'lucide-react';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 5 * 60 * 1000,
    },
  },
});

const ScreenerAppContent: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'screener' | 'ipo'>('screener');
  const [watchlistToast, setWatchlistToast] = useState<string | null>(null);

  const { data: revisionData } = useQuery({
    queryKey: ['revisionCurrent'],
    queryFn: () => screenerApi.getCurrentRevision(),
  });

  const selectedAsOfDate = revisionData?.latestSessionDate || '2026-09-28';

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
            onAddToWatchlist={handleAddToWatchlist}
          />
        ) : (
          <NewListingsTab />
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
