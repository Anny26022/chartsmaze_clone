import requests
import json
from datetime import datetime, timedelta
import os
from pipeline_utils import get_headers

def fetch_corporate_actions_scenarios():
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"
    
    # Calculate dates in IST
    utc_now = datetime.utcnow()
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    today_str = ist_now.strftime("%Y-%m-%d")
    two_years_ago = (ist_now - timedelta(days=365 * 2)).strftime("%Y-%m-%d")
    two_months_forward = (ist_now + timedelta(days=60)).strftime("%Y-%m-%d")

    headers = get_headers(include_origin=True)

    # Common parameters
    common_fields = ["CorpAct.ActType", "Sym", "DispSym", "CorpAct.ExDate", "CorpAct.RecDate", "CorpAct.Note"]
    
    def get_data(start_date, end_date):
        payload = {
            "data": {
                "sort": "CorpAct.ExDate",
                "sorder": "asc",
                "count": 5000,
                "fields": common_fields,
                "params": [
                    {"field": "Seg", "op": "", "val": "E"},
                    {"field": "OgInst", "op": "", "val": "ES"},
                    {"field": "CorpAct.ExDate", "op": "lte", "val": end_date},
                    {"field": "CorpAct.ExDate", "op": "gte", "val": start_date},
                    {"field": "Mcapclass", "op": "", "val": "Largecap,Midcap,Smallcap,Microcap"},
                    {"field": "CorpAct.ActType", "op": "", "val": "BONUS,DIVIDEND,QUARTERLY RESULT ANNOUNCEMENT,SPLIT,RIGHTS,BUYBACK"}
                ],
                "pgno": 0
            }
        }
        resp = requests.post(url, json=payload, headers=headers)
        resp.raise_for_status()
        raw_data = resp.json().get('data', [])
        
        # Flatten the nested CorpAct array
        flattened = []
        for stock in raw_data:
            symbol = stock.get('Sym')
            name = stock.get('DispSym')
            actions = stock.get('CorpAct', [])
            for action in actions:
                ex_date = action.get('ExDate')
                # Strict filtering by the intended range
                if start_date <= ex_date <= end_date:
                    flattened.append({
                        "Symbol": symbol,
                        "Name": name,
                        "Type": action.get('ActType'),
                        "ExDate": ex_date,
                        "RecordDate": action.get('RecDate'),
                        "Details": action.get('Note')
                    })
        # Sort by date
        return sorted(flattened, key=lambda x: x['ExDate'])

    print(f"Scenario 1: Fetching Historical Data ({two_years_ago} to Yesterday)...")
    yesterday = (ist_now - timedelta(days=1)).strftime("%Y-%m-%d")
    history = get_data(two_years_ago, yesterday)
    with open("history_corporate_actions.json", "w") as f:
        json.dump(history, f, indent=4)
    print(f"Saved {len(history)} historical actions to history_corporate_actions.json")

    print(f"Scenario 2: Fetching Upcoming Data (Today onwards for 2 months)...")
    upcoming = get_data(today_str, two_months_forward)
    with open("upcoming_corporate_actions.json", "w") as f:
        json.dump(upcoming, f, indent=4)
    print(f"Saved {len(upcoming)} upcoming actions to upcoming_corporate_actions.json")

if __name__ == "__main__":
    fetch_corporate_actions_scenarios()
