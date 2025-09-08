# main.py  (Cloud Functions Gen2 HTTP entry)
# - Exposes run_update(request) for --entry-point run_update
# - Calls your pipeline to update CSVs in GCS

import json
from flask import jsonify, Request  # Cloud Functions provides Flask request
from pipeline.update_daily import update_all  # your existing updater

def run_update(request: Request):
    """HTTP-triggered function: runs the daily updater once."""
    try:
        result = update_all()  # read->merge->rewrite per symbol
        return jsonify({"ok": True, "update": result}), 200
    except Exception as e:
        # Return error so health checks/logs are obvious
        return jsonify({"ok": False, "error": str(e)}), 502
