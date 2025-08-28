# HTTP Function: append daily bars, then publish MACO signals to BigQuery
import json
from pipeline.update_daily import update_all
from pipeline.publish_signals import publish_all

def run_update(request):
    upd = update_all()        # append new daily data to CSVs
    pub = publish_all()       # compute MACO + write to BigQuery
    body = {"ok": True, "update": upd, "publish": pub}
    return (json.dumps(body), 200, {"Content-Type": "application/json"})
