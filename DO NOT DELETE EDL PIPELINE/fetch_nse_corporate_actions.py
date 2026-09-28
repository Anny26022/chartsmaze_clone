"""Fetch the official NSE corporate-action ledger used by the EDL.

This is deliberately separate from the ScanX earnings-event fallback.  NSE is
the source of record for corporate actions; ScanX result announcements are
handled by ``fetch_corporate_actions.py`` because they are a different feed.
"""

from datetime import date, datetime, timedelta
from hashlib import sha256
import os
import re
import time

import requests

from pipeline_utils import BASE_DIR, load_json, save_json


PAGE_URL = "https://www.nseindia.com/companies-listing/corporate-filings-actions?tabIndex=equity"
API_URL = "https://www.nseindia.com/api/corporates-corporateActions"
DEFAULT_FROM = "2018-01-01"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36"

CATEGORY_RULES = (
    ("split", r"face value split|sub[ -]?division|stock split"),
    ("bonus", r"\bbonus\b"),
    ("capital-reduction", r"capital reduction|reduction of capital"),
    ("consolidation", r"\bconsolidat(?:e|ion)"),
    ("demerger", r"de[ -]?merger"),
    ("merger", r"\bmerger\b"),
    ("amalgamation", r"\bamalgamat"),
    ("scheme", r"scheme|arrangement"),
    ("rights", r"\brights?\b"),
    ("buyback", r"\bbuy[ -]?back\b"),
    ("dividend", r"\b(?:dividend|dividned|divdend|div)\b"),
    ("conversion", r"\bconvers(?:ion|ion of|e)"),
)
MANUAL_CATEGORIES = {"capital-reduction", "demerger", "merger", "amalgamation", "scheme", "conversion"}


def iso_today():
    return date.today().isoformat()


def nse_date(value):
    return date.fromisoformat(value).strftime("%d-%m-%Y")


def parse_nse_date(value):
    value = str(value or "").strip()
    for pattern in ("%d-%b-%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            pass
    return None


def categories_for(subject):
    return [category for category, pattern in CATEGORY_RULES if re.search(pattern, subject, flags=re.I)]


def adjustment_for(subject, categories):
    components = []
    if "split" in categories or "consolidation" in categories:
        match = re.search(
            r"from\s+(?:rs\.?|re\.?)?\s*(\d+(?:\.\d+)?)\s*\/?-?\s*(?:per share)?\s*to\s+(?:rs\.?|re\.?)?\s*(\d+(?:\.\d+)?)",
            subject,
            flags=re.I,
        )
        if match:
            old, new = map(float, match.groups())
            if old > 0 and new > 0:
                components.append((old / new, f"face value {old:g} to {new:g}"))
    if "bonus" in categories and not re.search(r"ncrps|debenture|warrant", subject, flags=re.I):
        match = re.search(r"\bbonus\s*[-:]?\s*(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", subject, flags=re.I)
        if match:
            issued, held = map(float, match.groups())
            if issued > 0 and held > 0:
                components.append(((held + issued) / held, f"bonus {issued:g}:{held:g}"))

    expected = int("split" in categories or "consolidation" in categories) + int("bonus" in categories)
    automatic = all(category in {"split", "bonus", "consolidation", "dividend"} for category in categories)
    if expected and automatic and len(components) == expected:
        share_factor = 1.0
        for factor, _ in components:
            share_factor *= factor
        return {
            "mode": "deterministic",
            "shareFactor": round(share_factor, 10),
            "priceFactor": round(1 / share_factor, 10),
            "basis": "; ".join(basis for _, basis in components),
        }
    if any(category in MANUAL_CATEGORIES for category in categories):
        return {"mode": "manual-review"}
    return {"mode": "context-only"}


def stable_id(action):
    return sha256("|".join((action["symbol"], action.get("isin", ""), action["exDate"], action["subject"])).encode()).hexdigest()[:20]


def fetch_nse_actions(from_date, to_date, session=None):
    session = session or requests.Session()
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json, text/plain, */*", "Referer": PAGE_URL}
    proxy_key = os.getenv("NSE_CORPORATE_ACTIONS_SCRAPER_API_KEY", "").strip()

    def request_url(url, params=None):
        if not proxy_key:
            return session.get(url, params=params, headers=headers, timeout=60)
        prepared = requests.Request("GET", url, params=params).prepare().url
        return session.get("https://api.scraperapi.com/", params={
            "api_key": proxy_key, "url": prepared, "country_code": "in", "device_type": "desktop", "keep_headers": "true",
        }, headers=headers, timeout=60)

    page = request_url(PAGE_URL)
    page.raise_for_status()
    response = request_url(API_URL, {
        "index": "equities", "from_date": nse_date(from_date), "to_date": nse_date(to_date),
    })
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("NSE corporate-actions response was not a list")
    return payload


def normalize_actions(rows):
    actions = []
    for row in rows:
        if str(row.get("series") or "").strip().upper() != "EQ":
            continue
        subject = " ".join(str(row.get("subject") or "").split())
        categories = categories_for(subject)
        symbol = str(row.get("symbol") or "").strip().upper()
        ex_date = parse_nse_date(row.get("exDate"))
        if not symbol or not ex_date or not categories:
            continue
        action = {
            "symbol": symbol,
            "company": str(row.get("comp") or "").strip(),
            "isin": str(row.get("isin") or "").strip().upper(),
            "series": "EQ",
            "categories": categories,
            "subject": subject,
            "exDate": ex_date,
            "recordDate": parse_nse_date(row.get("recDate")),
            "faceValue": _finite_number(row.get("faceVal")),
            "adjustment": adjustment_for(subject, categories),
        }
        actions.append({"id": stable_id(action), **action})
    return sorted({action["id"]: action for action in actions}.values(), key=lambda action: (action["exDate"], action["symbol"], action["id"]))


def _finite_number(value):
    try:
        number = float(value)
        return number if number == number and abs(number) != float("inf") else None
    except (TypeError, ValueError):
        return None


def build_outputs(rows, from_date, to_date, generated_at=None):
    actions = normalize_actions(rows)
    generated_at = generated_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    counts = {category: sum(category in action["categories"] for action in actions)
              for category, _ in CATEGORY_RULES}
    runtime = [action for action in actions if action["adjustment"]["mode"] != "context-only"]
    full = {
        "version": 1,
        "source": API_URL,
        "generated_at": generated_at,
        "range": {"from": from_date, "to": to_date},
        "counts": {"raw_nse_rows": len(rows), "retained_actions": len(actions), "categories": counts},
        "actions": actions,
    }
    adjustments = {
        "version": 1,
        "source": API_URL,
        "revision": sha256(json_bytes(runtime)).hexdigest(),
        "generated_at": generated_at,
        "range": full["range"],
        "actions": runtime,
    }
    return full, adjustments


def json_bytes(value):
    import json
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def main():
    from_date = os.getenv("NSE_CORPORATE_ACTIONS_FROM", DEFAULT_FROM)
    to_date = os.getenv("NSE_CORPORATE_ACTIONS_TO", (date.today() + timedelta(days=365)).isoformat())
    source_file = os.getenv("NSE_CORPORATE_ACTIONS_SOURCE_FILE", "").strip()
    if source_file:
        source = load_json(source_file)
        rows = source.get("actions", source) if isinstance(source, dict) else source
        if isinstance(source, dict) and source.get("source") == API_URL:
            save_json("nse_corporate_actions.json", source)
            runtime = {key: source.get(key) for key in ("version", "source", "generated_at", "range")}
            runtime_actions = [action for action in source.get("actions", []) if action.get("adjustment", {}).get("mode") != "context-only"]
            runtime.update({"revision": sha256(json_bytes(runtime_actions)).hexdigest(), "actions": runtime_actions})
            save_json("nse_corporate_action_adjustments.json", runtime)
            return True
    else:
        rows = fetch_nse_actions(from_date, to_date)
    if not isinstance(rows, list) or not rows:
        raise ValueError("NSE corporate-actions source was empty; preserving published data")
    full, adjustments = build_outputs(rows, from_date, to_date)
    save_json("nse_corporate_actions.json", full)
    save_json("nse_corporate_action_adjustments.json", adjustments)
    print(f"Saved {len(full['actions'])} official NSE corporate actions; {len(adjustments['actions'])} runtime actions.")
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
