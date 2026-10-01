import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from edl_pipeline.scanner.financials import financial_value


class FinancialScannerTests(unittest.TestCase):
    def context(self):
        rows=[]
        for statement,profits in [("CONSOLIDATED",[10,20,30,40]),("STANDALONE",[5,5,5,5])]:
            for quarter,profit in zip(["2025-09-30","2025-12-31","2026-03-31","2026-06-30"],profits):
                rows.append({"quarter_end":quarter,"filing_date":"2026-08-01","report_type":statement,"net_profit":profit})
        return {"financial_history":{"TEST":rows},"financial_history_as_of":"2026-09-30"}

    def test_pe_selects_statement_and_requires_four_consecutive_quarters(self):
        context=self.context()
        spec={"condition":"pe_ratio","report_type":"PREFER_CONSOLIDATED"}
        self.assertEqual(financial_value(context,{"symbol":"TEST"},spec,date(2026,9,30),1000)[0],10)
        spec["report_type"]="STANDALONE"
        self.assertEqual(financial_value(context,{"symbol":"TEST"},spec,date(2026,9,30),1000)[0],50)
        context["financial_history"]["TEST"].pop()
        self.assertIsNone(financial_value(context,{"symbol":"TEST"},spec,date(2026,9,30),1000)[0])

    def test_growth_uses_same_statement_comparison_quarter(self):
        spec={"condition":"earnings_growth","report_type":"CONSOLIDATED","metric":"net_profit","basis":"qoq","maximum_filing_age_days":200}
        value,details,reason=financial_value(self.context(),{"symbol":"TEST"},spec,date(2026,9,30),1000)
        self.assertAlmostEqual(value,100/3)
        self.assertEqual(details["report_type"],"CONSOLIDATED")
        self.assertIsNone(reason)

    def test_future_filings_and_unobserved_historical_revisions_are_unavailable(self):
        spec={"condition":"pe_ratio","report_type":"CONSOLIDATED"}
        self.assertIsNone(financial_value(self.context(),{"symbol":"TEST"},spec,date(2026,7,1),1000)[0])
        self.assertIsNone(financial_value(self.context(),{"symbol":"TEST"},spec,date(2026,9,29),1000)[0])

    def test_nonpositive_ttm_profit_is_unavailable(self):
        context=self.context()
        for row in context["financial_history"]["TEST"]:
            row["net_profit"]=-10
        value,_,reason=financial_value(context,{"symbol":"TEST"},{"condition":"pe_ratio"},date(2026,9,30),1000)
        self.assertIsNone(value)
        self.assertEqual(reason,"positive_ttm_profit_or_market_cap_unavailable")
