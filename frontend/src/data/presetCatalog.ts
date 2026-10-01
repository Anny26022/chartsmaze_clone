import { ConditionDef, ConditionCategory } from '../types/screener';
import definitions from './presetDefinitions.json';

const categories: Record<string, ConditionCategory> = {
  'Momentum & Leadership': 'momentum', Breakouts: 'trend',
  'Bases & Contraction': 'range', 'Pullbacks & Reclaims': 'trend',
  'Volume & Delivery': 'momentum', Gaps: 'momentum', Reversals: 'relative_strength',
  'Earnings & Value': 'fundamentals', 'Regime & Universe': 'liquidity',
};

export const PRESET_CATALOG: ConditionDef[] = definitions.map(p => ({
  id: p.id, label: p.name, category: categories[p.category] ?? 'trend',
  description: p.rules.join('; '), parameters: [],
}));
