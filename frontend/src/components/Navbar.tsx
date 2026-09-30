import React from 'react';
import { SlidersHorizontal, Sparkles } from 'lucide-react';

interface NavbarProps {
  activeTab: 'screener' | 'ipo';
  onTabChange: (tab: 'screener' | 'ipo') => void;
}

export const Navbar: React.FC<NavbarProps> = ({ activeTab, onTabChange }) => (
  <header className="bg-white border-b border-gray-200 sticky top-0 z-40">
    <div className="max-w-screen-xl mx-auto px-6 h-14 flex items-center justify-between">
      {/* Logo */}
      <div className="flex items-center gap-2.5">
        <div className="w-7 h-7 rounded-lg bg-gray-900 flex items-center justify-center text-white text-[11px] font-black tracking-tight">
          NX
        </div>
        <span className="text-sm font-bold text-gray-900">
          Nexus <span className="font-normal text-gray-500">Screener</span>
        </span>
      </div>

      {/* Tab Navigation */}
      <nav className="flex items-center bg-gray-100 rounded-xl p-1 gap-0.5">
        <button
          onClick={() => onTabChange('screener')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-lg text-xs font-medium transition-all cursor-pointer ${
            activeTab === 'screener'
              ? 'bg-white text-gray-900 shadow-sm font-semibold'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          <SlidersHorizontal className="w-3.5 h-3.5" />
          Screener
        </button>
        <button
          onClick={() => onTabChange('ipo')}
          className={`flex items-center gap-1.5 px-4 py-1.5 rounded-lg text-xs font-medium transition-all cursor-pointer ${
            activeTab === 'ipo'
              ? 'bg-white text-gray-900 shadow-sm font-semibold'
              : 'text-gray-500 hover:text-gray-700'
          }`}
        >
          <Sparkles className="w-3.5 h-3.5 text-amber-500" />
          IPO Catalogue
        </button>
      </nav>
    </div>
  </header>
);
