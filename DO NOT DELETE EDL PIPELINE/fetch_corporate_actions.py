import sys
from datetime import datetime, timedelta

from pipeline_utils import fetch_scanx_data, save_json


ACTION_FIELDS = ["CorpAct.ActType", "Sym", "DispSym", "CorpAct.ExDate", "CorpAct.RecDate", "CorpAct.Note"]
ACTION_TYPES = "BONUS,DIVIDEND,QUARTERLY RESULT ANNOUNCEMENT,SPLIT,RIGHTS,BUYBACK"


def build_payload(start_date, end_date):
    return {
        "data": {
            "sort": "CorpAct.ExDate",
            "sorder": "asc",
            "count": 5000,
            "fields": ACTION_FIELDS,
            "params": [
                {"field": "Seg", "op": "", "val": "E"},
                {"field": "OgInst", "op": "", "val": "ES"},
                {"field": "CorpAct.ExDate", "op": "lte", "val": end_date},
                {"field": "CorpAct.ExDate", "op": "gte", "val": start_date},
                {"field": "Mcapclass", "op": "", "val": "Largecap,Midcap,Smallcap,Microcap"},
                {"field": "CorpAct.ActType", "op": "", "val": ACTION_TYPES},
            ],
            "pgno": 0,
        }
    }


def flatten_actions(raw_data, start_date, end_date):
    flattened = []
    for stock in raw_data:
        symbol = stock.get("Sym")
        name = stock.get("DispSym")
        for action in stock.get("CorpAct", []):
            ex_date = action.get("ExDate")
            if ex_date and start_date <= ex_date <= end_date:
                flattened.append({
                    "Symbol": symbol,
                    "Name": name,
                    "Type": action.get("ActType"),
                    "ExDate": ex_date,
                    "RecordDate": action.get("RecDate"),
                    "Details": action.get("Note"),
                })
    return sorted(flattened, key=lambda x: x["ExDate"])


def fetch_actions(start_date, end_date):
    return flatten_actions(fetch_scanx_data(build_payload(start_date, end_date)), start_date, end_date)

def fetch_corporate_actions_scenarios():
    # Calculate dates in IST
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    today_str = ist_now.strftime("%Y-%m-%d")
    two_years_ago = (ist_now - timedelta(days=365 * 2)).strftime("%Y-%m-%d")
    two_months_forward = (ist_now + timedelta(days=60)).strftime("%Y-%m-%d")

    print(f"Scenario 1: Fetching Historical Data ({two_years_ago} to Yesterday)...")
    yesterday = (ist_now - timedelta(days=1)).strftime("%Y-%m-%d")
    history = fetch_actions(two_years_ago, yesterday)
    save_json("history_corporate_actions.json", history)
    print(f"Saved {len(history)} historical actions to history_corporate_actions.json")

    print(f"Scenario 2: Fetching Upcoming Data (Today onwards for 2 months)...")
    upcoming = fetch_actions(today_str, two_months_forward)
    save_json("upcoming_corporate_actions.json", upcoming)
    print(f"Saved {len(upcoming)} upcoming actions to upcoming_corporate_actions.json")
    return True

if __name__ == "__main__":
    sys.exit(0 if fetch_corporate_actions_scenarios() else 1)
