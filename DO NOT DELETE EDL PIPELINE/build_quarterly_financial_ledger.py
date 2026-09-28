"""Publish dated quarterly fundamentals without inventing filing dates.

ScanX's fundamental response supplies the numeric quarterly series while its
LODR/company-filings feeds supply the time a result was disclosed.  Neither
source alone is enough for an auditable historical ledger, so this joins only
unambiguous quarter ends to result disclosures.  The source statement type is
kept explicit: a standalone figure never silently substitutes for a missing
consolidated figure.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pipeline_utils import BASE_DIR, load_json, save_json


STATEMENTS = (("incomeStat_cq", "CONSOLIDATED"), ("incomeStat_sq", "STANDALONE"))
METRICS = ("REVENUE", "SALES", "NET_PROFIT", "PROFIT_BEFORE_TAX", "EPS", "OPM", "EBITDA")
RESULT_TERMS = ("financial results", "financial result", "unaudited financial", "audited financial")
DATE_PATTERNS = (
    re.compile(r"(?:quarter|quarterly|results?).{0,120}?(?:ended|ending)\s*(?:on\s*)?"
               r"([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I),
    re.compile(r"(?:ended|ending)\s*(?:on\s*)?"
               r"([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}-\d{2}-\d{2})", re.I),
)


def _quarter_end(text: str) -> str | None:
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        value = re.sub(r"\s+", " ", match.group(1).replace(",", " ")).strip()
        for fmt in ("%B %d %Y", "%b %d %Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                pass
    return None


def _is_result(filing: dict) -> bool:
    text = " ".join(str(filing.get(key) or "") for key in ("descriptor", "caption", "news_body")).lower()
    return any(term in text for term in RESULT_TERMS)


def _filing_candidates(filings: list[dict]) -> dict[str, dict]:
    """Return the first actual disclosure for each stated quarter end."""
    candidates: dict[str, list[dict]] = defaultdict(list)
    seen = set()
    for filing in filings:
        if not isinstance(filing, dict) or not _is_result(filing):
            continue
        text = " ".join(str(filing.get(key) or "") for key in ("descriptor", "caption", "news_body"))
        quarter_end = _quarter_end(text)
        published = str(filing.get("news_date") or "")
        if not quarter_end or not published:
            continue
        # LODR and the legacy feed frequently expose the same attachment under
        # different ids.  Its URL is the strongest available duplicate key.
        key = str(filing.get("file_url") or "").strip() or (quarter_end, published[:10], text[:160])
        if key in seen:
            continue
        seen.add(key)
        candidates[quarter_end].append(filing)
    return {
        quarter: min(rows, key=lambda row: str(row.get("news_date") or ""))
        for quarter, rows in candidates.items()
    }


def _series(statement: dict, metric: str) -> list[str]:
    value = statement.get(metric)
    return str(value).split("|") if value not in (None, "") else []


def _value(statement: dict, metric: str, index: int):
    values = _series(statement, metric)
    if index >= len(values) or values[index] in ("", "-", "N/A"):
        return None
    try:
        return float(values[index].replace(",", ""))
    except ValueError:
        return None


def main() -> int:
    root = Path(BASE_DIR)
    fundamentals = load_json(root / "fundamental_data.json", default=[])
    filing_cache = load_json(root / "filing_history_data" / "filing_history.json", default={})
    symbols = filing_cache.get("symbols", {}) if isinstance(filing_cache, dict) else {}
    if not isinstance(fundamentals, list) or not isinstance(symbols, dict):
        print("Cannot build quarterly financial ledger without fundamentals and filing history.")
        return 1

    records = []
    unmatched_quarters = 0
    for item in fundamentals:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("Symbol") or "").upper()
        filing_entry = symbols.get(symbol, {})
        matches = _filing_candidates(filing_entry.get("filings", []) if isinstance(filing_entry, dict) else [])
        for statement_key, report_type in STATEMENTS:
            statement = item.get(statement_key)
            if not isinstance(statement, dict):
                continue
            years = _series(statement, "YEAR")
            for index, year_month in enumerate(years):
                if not re.fullmatch(r"\d{6}", year_month):
                    continue
                quarter_end = f"{year_month[:4]}-{year_month[4:]}-" + {"03": "31", "06": "30", "09": "30", "12": "31"}.get(year_month[4:], "")
                if quarter_end.endswith("-"):
                    continue
                filing = matches.get(quarter_end)
                if filing is None:
                    unmatched_quarters += 1
                    continue
                values = {metric.lower(): _value(statement, metric, index) for metric in METRICS}
                if all(value is None for value in values.values()):
                    continue
                records.append({
                    "symbol": symbol,
                    "isin": item.get("isin") or item.get("ISIN"),
                    "quarter_end": quarter_end,
                    "filing_date": str(filing.get("news_date")),
                    "report_type": report_type,
                    **values,
                    "filing_descriptor": filing.get("descriptor"),
                    "filing_caption": filing.get("caption"),
                    "filing_url": filing.get("file_url"),
                    "numeric_source": "ScanX fundamental quarterly statement",
                    "filing_source": "ScanX LODR/company_filings",
                })

    records.sort(key=lambda row: (row["symbol"], row["quarter_end"], row["report_type"]))
    coverage = {
        "symbols_with_rows": len({row["symbol"] for row in records}),
        "records": len(records),
        "consolidated_records": sum(row["report_type"] == "CONSOLIDATED" for row in records),
        "standalone_records": sum(row["report_type"] == "STANDALONE" for row in records),
        "unmatched_numeric_quarters": unmatched_quarters,
    }
    save_json(root / "quarterly_financial_history.json", {
        "schema_version": 1,
        "source": "ScanX quarterly statements joined to dated ScanX LODR/company-filings disclosures",
        "temporal_contract": "Filing dates are source disclosure timestamps. Numeric values are current provider series matched by quarter; consumers must not back-project them without accepting that source-observation limitation.",
        "coverage": coverage,
        "records": records,
    }, ensure_ascii=False)
    print(f"Published {len(records)} dated quarterly financial rows for {coverage['symbols_with_rows']} symbols.")
    # Filing metadata can be present before a result notice has a parseable
    # quarter end.  Publish that coverage state instead of failing an otherwise
    # healthy market-data refresh.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
