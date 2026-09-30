import { ConditionDef } from '../types/screener';

export const PRESET_CATALOG: ConditionDef[] = [
  {
    id: 'preset_persistent_momentum',
    label: 'Persistent Momentum',
    category: 'momentum',
    description: 'Persistent above EMA 10 for 20d, EMA 20 for 30d, or EMA 50 for 50d; 20d average turnover > ₹5Cr.',
    parameters: []
  },
  {
    id: 'preset_easy_money',
    label: 'Easy Money',
    category: 'momentum',
    description: '5d return >20%; 21d >25%; 63d >35%; 126d >50%; new 52-week high; ADR(14) >6%; RVOL(20) >3x; gap up >5%; or 1d return >6%.',
    parameters: []
  },
  {
    id: 'preset_relative_strength_leaders',
    label: 'Relative Strength Leaders',
    category: 'momentum',
    description: '60d RS vs Nifty 50 >15%; within 15% of 52-week high; turnover >₹5Cr.',
    parameters: []
  },
  {
    id: 'preset_stage_2_uptrend',
    label: 'Stage 2 Uptrend',
    category: 'momentum',
    description: 'Above 50 EMA for 40d; above 200 EMA for 60d; >30% above 52-week low; within 25% of 52-week high.',
    parameters: []
  },
  {
    id: 'preset_momentum_burst',
    label: 'Momentum Burst',
    category: 'momentum',
    description: '3d return >8%; RVOL(20) >2x; ADR(14) >3%; turnover >₹5Cr.',
    parameters: []
  },
  {
    id: 'preset_52_week_high_breakout',
    label: '52-Week High Breakout',
    category: 'trend',
    description: 'New 252-session high; RVOL(20) >1.5x; turnover >₹5Cr.',
    parameters: []
  },
  {
    id: 'preset_vcp_contraction',
    label: 'VCP Contraction',
    category: 'range',
    description: '10d range ≤0.5× prior 60d range; 5d volume ≤0.8× 50d volume; within 20% of 52-week high; turnover >₹3Cr.',
    parameters: []
  },
  {
    id: 'preset_earnings_growth_momentum',
    label: 'Earnings Growth Momentum',
    category: 'fundamentals',
    description: 'Consolidated-preferred quarterly net-profit YoY growth >25%, report no older than 200d; within 20% of 52-week high.',
    parameters: []
  }
];
