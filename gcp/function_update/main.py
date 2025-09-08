# main.py
# Purpose: Cloud Run entrypoint (HTTP or console). Calls update_all().

import json
from pipeline.update_daily import update_all

def run_once():
    """CLI entry; prints JSON to stdout."""
    print(json.dumps({"ok": True, "update": update_all()}, default=str))

# If deployed as a simple HTTP service (Functions Framework / Flask),
# you can optionally expose an HTTP handler:
try:
    # Optional: only if functions-framework installed
    from flask import Flask, jsonify
    app = Flask(__name__)

    @app.get("/")
    def root():
        try:
            res = update_all()
            return jsonify({"ok": True, "update": res}), 200
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 502
except Exception:
    # If Flask not installed, ignore – CLI mode still works.
    pass

if __name__ == "__main__":
    run_once()
