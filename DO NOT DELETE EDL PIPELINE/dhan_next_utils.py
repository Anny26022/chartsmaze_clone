import json
import re

import requests

from pipeline_utils import get_headers


def user_agent_headers():
    return {"User-Agent": get_headers()["User-Agent"]}


def get_build_id(page_url):
    """Fetch the current Dhan Next.js build id from any rendered Dhan page."""
    try:
        response = requests.get(page_url, headers=user_agent_headers(), timeout=10)
        match = re.search(r'"buildId":"([^"]+)"', response.text)
        return match.group(1) if match else None
    except Exception:
        return None


def get_next_data(build_id, page_path, timeout=15):
    if not build_id:
        return {}
    url = f"https://dhan.co/_next/data/{build_id}/{page_path}.json"
    try:
        response = requests.get(url, headers=user_agent_headers(), timeout=timeout)
        return response.json() if response.status_code == 200 else {}
    except Exception:
        return {}


def get_embedded_next_data(page_url, timeout=15):
    try:
        from bs4 import BeautifulSoup

        response = requests.get(page_url, headers=user_agent_headers(), timeout=timeout)
        soup = BeautifulSoup(response.text, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        return json.loads(script.string) if script else {}
    except Exception:
        return {}


def find_nested_list(obj, predicate):
    if isinstance(obj, list) and obj and predicate(obj):
        return obj
    if isinstance(obj, dict):
        for value in obj.values():
            result = find_nested_list(value, predicate)
            if result:
                return result
    return None
