import React, { useState } from 'react';

interface SymbolWithLogoProps {
  symbol: string;
  name?: string;
  size?: 'sm' | 'md';
  showText?: boolean;
  className?: string;
}

const companyLogo = (symbol: string) => `https://images.dhan.co/symbol/${encodeURIComponent(symbol.trim().toUpperCase())}.png`;
const indiaFallback = 'https://s3-symbol-logo.tradingview.com/country/IN.svg';

/** Nexus Ultimate's compact Dhan-first company mark with an India fallback. */
export const SymbolWithLogo: React.FC<SymbolWithLogoProps> = ({ symbol, name, size = 'sm', showText = true, className = '' }) => {
  const [source, setSource] = useState<'company' | 'country' | 'none'>('company');
  const dimension = size === 'md' ? 'h-6 w-6' : 'h-4 w-4';
  const src = source === 'company' ? companyLogo(symbol) : source === 'country' ? indiaFallback : null;
  return <span className={`inline-flex min-w-0 items-center gap-2 ${className}`}>
    {src ? <img src={src} alt="" className={`${dimension} shrink-0 rounded object-contain`} loading="lazy" onError={() => setSource(current => current === 'company' ? 'country' : 'none')} />
      : <span className={`${dimension} shrink-0 rounded bg-slate-100 text-center text-[9px] font-bold leading-4 text-slate-500`}>{symbol.slice(0, 1)}</span>}
    {showText && <span className="min-w-0"><b className="block truncate text-xs text-slate-900">{symbol}</b>{name && <span className="block truncate text-[11px] text-slate-500">{name}</span>}</span>}
  </span>;
};
