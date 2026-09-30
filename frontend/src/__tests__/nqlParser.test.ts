import { describe, it, expect } from 'vitest';
import { explainCondition, explainExpressionTree, parseNqlQuery } from '../utils/nqlParser';
import { ExpressionGroupNode, ActiveCondition } from '../types/screener';

describe('Nexus Query Language (NQL) & Expression Serialization', () => {
  it('should serialize trend_price_vs_ma condition into human-readable explanation', () => {
    const condition: ActiveCondition = {
      instanceId: 'c1',
      conditionId: 'trend_price_vs_ma',
      parameters: { maType: 'SMA', maPeriod: 50, operator: 'above', thresholdPct: 0 },
    };

    const res = explainCondition(condition);
    expect(res.explanation).toContain('Price is ABOVE SMA 50');
    expect(res.isAvailable).toBe(true);
  });

  it('should handle negated condition serialization', () => {
    const condition: ActiveCondition = {
      instanceId: 'c2',
      conditionId: 'mom_rvol',
      parameters: { minRvol: 2.0, maxRvol: 10.0 },
      isNegated: true,
    };

    const res = explainCondition(condition);
    expect(res.explanation).toContain('NOT (');
    expect(res.explanation).toContain('Relative Volume');
  });

  it('should evaluate data availability warnings for historical session dates', () => {
    const tree: ExpressionGroupNode = {
      type: 'group',
      operator: 'all',
      children: [
        {
          type: 'condition',
          condition: {
            instanceId: 'c3',
            conditionId: 'mom_delivery_pct',
            parameters: { minDeliveryPct: 50 },
          },
        },
      ],
    };

    // Session date prior to 2024-01-01 should trigger data unavailable warning
    const exp = explainExpressionTree(tree, '2023-11-15');
    expect(exp.compiledExplanations[0].isDataAvailableForSession).toBe(false);
    expect(exp.warnings.length).toBeGreaterThan(0);
    expect(exp.warnings[0]).toContain('relies on historical delivery data which is unavailable');
  });

  it('should parse NQL query string into ExpressionNode tree', () => {
    const query = '(close > sma50 AND rvol >= 1.5) OR change_pct > 3.0';
    const parsed = parseNqlQuery(query);

    expect(parsed.error).toBeNull();
    expect(parsed.tree).not.toBeNull();
    expect(parsed.tree?.type).toBe('group');
    if (parsed.tree?.type === 'group') {
      expect(parsed.tree.operator).toBe('any'); // Contains OR
      expect(parsed.tree.children.length).toBeGreaterThan(0);
    }
  });
});
