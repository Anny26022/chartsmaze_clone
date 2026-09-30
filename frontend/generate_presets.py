import json

presets_raw = """
Momentum & leadership
1. Persistent Momentum
Persistent above EMA 10 for 20d, EMA 20 for 30d, or EMA 50 for 50d; 20d average turnover > ₹5Cr.
2. Easy Money
5d return >20%; 21d >25%; 63d >35%; 126d >50%; new 52-week high; ADR(14) >6%; RVOL(20) >3x; gap up >5%; or 1d return >6%.
3. Relative Strength Leaders
60d RS vs Nifty 50 >15%; within 15% of 52-week high; turnover >₹5Cr.
4. RS Line at New High
RS vs Nifty 50 at a 60d high while price is no more than 2% below its high; turnover >₹5Cr.
5. Stage 2 Uptrend
Above 50 EMA for 40d; above 200 EMA for 60d; >30% above 52-week low; within 25% of 52-week high.
6. Momentum Burst
3d return >8%; RVOL(20) >2x; ADR(14) >3%; turnover >₹5Cr.
7. Quiet Strength
Above 20 EMA for 25d; ADR(14) <2.5%; 60d RS vs Nifty 50 >5%; turnover >₹5Cr.
Breakouts
8. 52-Week High Breakout
New 252-session high; RVOL(20) >1.5x; turnover >₹5Cr.
9. 20-Day High on Record Volume
New 20-session high; highest volume in 20 sessions; above 50 EMA for 20d.
10. Breakout from Tight Base
Prior 20d range ≤7%, excluding latest bar; new 20d high; RVOL(20) >2x; turnover >₹3Cr.
11. Pocket Pivot
RVOL(20) >1.5x; 1d return >1%; above 50 EMA for 10d; turnover >₹3Cr.
12. ADX Trend Breakout
ADX(14) >25; new 50d high; turnover >₹5Cr.
13. Circuit-Safe Breakout
New 60d high; circuit band ≥20%; turnover >₹10Cr.
Bases & contraction
14. VCP Contraction
10d range ≤0.5× prior 60d range; 5d volume ≤0.8× 50d volume; within 20% of 52-week high; turnover >₹3Cr.
15. Inside Bar Coil
Two consecutive daily inside bars; above 20 EMA for 5d; turnover >₹3Cr.
16. Weekly Inside Bar
One weekly inside bar; within 15% of 52-week high; market cap >₹1,000Cr.
17. Volume Dry-Up Base
5d volume ≤0.6× 50d volume; 15d range ≤6%; within 25% of 52-week high.
18. Flat Base
30d range ≤10%; within 12% of 52-week high; above 50 EMA for 30d.
19. Horizontal Resistance
252d resistance analysis: ≥1.5% swings, highs clustered within 2.5%, base ≥15d, price ≤5% below resistance and ≤2% below 20 EMA; EMA stack 10 > 20 > 50; turnover >₹5Cr.
20. Flags & Pennants
30d return >20%; 7d range ≤0.5× prior 25d range; 5d volume ≤0.8× prior 25d volume; above 21 EMA; turnover >₹5Cr.
21. Low-ATR Coil
ATR(14) <2%; 10d range ≤4%; turnover >₹3Cr.
Pullbacks & reclaims
22. 21 EMA Pullback
21 EMA shakeout/reclaim within 3d; above 50 EMA for 20d; 60d RS vs Nifty 50 >5%.
23. 50 EMA Shakeout
50 EMA shakeout/reclaim within 5d; within 25% of 52-week high; turnover >₹5Cr.
24. Higher-Low Pullback
10d price decline of at least 4%; above 50 EMA for 40d; within 20% of 52-week high.
25. Gap Support Retest
Unfilled upward gap ≥3% within 30d; above 20 EMA for 3d; turnover >₹3Cr.
Volume & delivery
26. Volume Surge
RVOL(20) >2x; 1d return >2%.
27. Highest Volume in 3 Months
Highest volume in 60d; 1d return >3%; turnover >₹3Cr.
28. Delivery-Backed Accumulation
Delivery percentage ≥60%; RVOL(20) >1.5x; above 20 EMA for 5d.
29. Sustained Accumulation
10d average volume ≥1.5× 60d average volume; 20d return >10%; turnover >₹3Cr.
Gaps
30. Unfilled Gap Up
Unfilled upward gap ≥4% within 60d; above 20 EMA for 3d; turnover >₹5Cr.
31. Gap & Go
Current gap up ≥3%; RVOL(20) >2x; turnover >₹5Cr.
32. Gap Down Washout
Current gap down ≥4%; within 10% of 52-week low; turnover >₹5Cr.
Reversals
33. 52-Week Low Bounce
Within 8% of 52-week low; 5d return >5%; RVOL(20) >1.5x.
34. RS Divergence Turn
60d RS vs Nifty 50 >0%; at least 30% below 52-week high; above 20 EMA for 5d.
35. Relative Weakness
60d RS vs Nifty 50 below −15%; new 60d low; turnover >₹10Cr.
Earnings & value
36. Earnings Growth Momentum
Consolidated-preferred quarterly net-profit YoY growth >25%, report no older than 200d; within 20% of 52-week high; turnover >₹5Cr.
37. Post-Earnings Drift
Earnings reported within 5d; 3d return >5%; RVOL(20) >2x.
38. Growth at a Fair Price
Consolidated-preferred P/E <30; YoY net-profit growth >20%, report ≤200d old; market cap >₹2,000Cr.
39. Revenue & Profit Acceleration
Consolidated-preferred revenue YoY >20%; net-profit YoY >20%; above 50 EMA for 20d.
40. Pre-Earnings Coil
More than 60d since latest earnings; 15d range ≤6%; within 20% of 52-week high.
Regime & universe
41. Breadth-Gated Leaders
Market breadth: >50% of active stocks above 50 SMA; persistent momentum; turnover >₹5Cr.
42. Liquid Trading Universe
Turnover >₹50Cr; price >₹100; EQ series; circuit band ≥20%.
43. Fresh IPO Base
Listed fewer than 400d ago; 20d range ≤10%; turnover >₹5Cr.
44. Nifty 500 Momentum
Nifty 500 constituent; 60d RS vs Nifty 50 >10%; within 15% of 52-week high.
45. Midcap Breakout
Market cap >₹2,000Cr; new 60d high; RVOL(20) >1.5x; turnover >₹10Cr.
"""

lines = presets_raw.strip().split('\n')
presets = []
current_cat = "trend"
current_item = None

cat_map = {
    "Momentum & leadership": "momentum",
    "Breakouts": "trend",
    "Bases & contraction": "range",
    "Pullbacks & reclaims": "trend",
    "Volume & delivery": "liquidity",
    "Gaps": "momentum",
    "Reversals": "relative_strength",
    "Earnings & value": "fundamentals",
    "Regime & universe": "liquidity",
}

for line in lines:
    line = line.strip()
    if not line:
        continue
    if line in cat_map:
        current_cat = cat_map[line]
        continue
    
    import re
    m = re.match(r'^\d+\.\s*(.+)', line)
    if m:
        if current_item:
            presets.append(current_item)
        label = m.group(1).strip()
        _id = "preset_" + re.sub(r'[^a-z0-9]+', '_', label.lower()).strip('_')
        current_item = {
            "id": _id,
            "label": label,
            "category": current_cat,
            "description": "",
            "parameters": []
        }
    else:
        if current_item:
            current_item["description"] += line + " "

if current_item:
    presets.append(current_item)

ts_content = "import { ConditionDef } from '../types/screener';\n\n"
ts_content += "export const NEXUS_CONDITION_CATALOG: ConditionDef[] = [\n"

for p in presets:
    desc = p['description'].strip().replace("'", "\\'")
    label = p['label'].replace("'", "\\'")
    
    ts_content += f"""  {{
    id: '{p["id"]}',
    label: '{label}',
    category: '{p["category"]}',
    description: '{desc}',
    parameters: []
  }},
"""

ts_content += "];\n"

with open('src/data/conditionCatalog.ts', 'w') as f:
    f.write(ts_content)

print("Generated src/data/conditionCatalog.ts")
