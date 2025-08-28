# Only define the HTTP handler. Do NOT run any code at import time.
# Does: append daily bars (Finnhub) -> compute MACO -> write signals to BigQuery

import json

def run_update(request):
    # Import heavy deps INSIDE the handler so startup never crashes on import.
    from pipeline.update_daily import update_all
    try:
        upd = update_all()              # append to gs://.../data/raw/*.csv
    except Exception as e:
        # Return JSON; container stays alive
        return (json.dumps({"ok": False, "stage": "update_all", "error": str(e)}),
                500, {"Content-Type": "application/json"})

    # Optional: publish MACO signals to BigQuery (Python path)
    try:
        from pipeline.publish_signals import publish_all
        pub = publish_all()             # write to <project>.<dataset>.maco_signals
    except Exception as e:
        return (json.dumps({"ok": False, "stage": "publish_all", "update": upd, "error": str(e)}),
                500, {"Content-Type": "application/json"})

    return (json.dumps({"ok": True, "update": upd, "publish": pub}),
            200, {"Content-Type": "application/json"})
