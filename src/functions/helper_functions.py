import json
import time

def _api_response(status_code, body):
    """Standardizes API Gateway responses."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }

def _normalize_event(event):
    """Handles both raw dictionary invokes and API Gateway string payloads."""
    if "body" in event:
        return json.loads(event["body"])
    return event

def _retry_gspread(func, retries=3, backoff=2):
    """Exponential backoff for Google API rate limits."""
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt == retries - 1:
                raise e
            time.sleep(backoff * (2 ** attempt))