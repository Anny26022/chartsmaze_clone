"""Persistent JSON-lines worker used by the local Vite API."""
import json
import sys
import traceback

from scanner_bridge import run
from scanner_cache import ScannerCache

cache = ScannerCache()
for line in sys.stdin:
    message = {}
    try:
        message = json.loads(line)
        payload = run(message['request'], cache=cache)
        response = {'id':message['id'], 'status':200, 'payload':payload}
    except (ValueError, KeyError, TypeError) as error:
        response = {'id':message.get('id'), 'status':422, 'payload':{'error':str(error)}}
    except Exception:
        traceback.print_exc(file=sys.stderr)
        response = {'id':message.get('id'), 'status':500, 'payload':{'error':'Local scanner failed. Check the terminal for diagnostics.'}}
    print(json.dumps(response, allow_nan=False), flush=True)
