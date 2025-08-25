# Cloud Function (Gen2) entrypoint. It:
# 1) Appends daily candles to each CSV in GCS.
# 2) Computes MACO (SMA20/50 by default) and a tiny 1-step forecast.
# 3) Writes summaries per symbol to GCS as JSON (for the dashboard).
import json, os, numpy as np, pandas as pd
from google.cloud import storage
from pipeline.update_daily import update_all, _gcs
from pipeline.config import SYMBOLS, SHORT_WINDOW, LONG_WINDOW, DATA_DIR

def _load(symbol):
    b = _gcs().bucket(os.getenv("GCS_BUCKET")).blob(f"{DATA_DIR}/{symbol}.csv")
    import io
    return pd.read_csv(io.BytesIO(b.download_as_bytes()), parse_dates=["time"])

def _sma(s, w): return s.rolling(w).mean()

def _forecast_next_close(series, lookback=20):
    y = series.dropna().values[-lookback:]
    if len(y) < 3: return None
    x = np.arange(len(y))
    slope, intercept = np.polyfit(x, y, 1)
    return float(intercept + slope * len(y))

def _write_json(obj, path):
    _gcs().bucket(os.getenv("GCS_BUCKET")).blob(path).upload_from_string(
        json.dumps(obj, indent=2), content_type="application/json"
    )

def compute_and_save_summaries():
    out = {}
    for sym in SYMBOLS:
        df = _load(sym).sort_values("time")
        df["SMA_S"] = _sma(df["c"], SHORT_WINDOW)
        df["SMA_L"] = _sma(df["c"], LONG_WINDOW)
        signal = 1 if df["SMA_S"].iloc[-1] > df["SMA_L"].iloc[-1] else -1
        next_close = _forecast_next_close(df["c"])
        last_close = float(df["c"].iloc[-1])
        out[sym] = {
            "last_time": str(df["time"].iloc[-1]),
            "last_close": last_close,
            "signal": signal,          # 1=long, -1=exit
            "next_close": next_close,  # simple next-point prediction
        }
        _write_json(out[sym], f"{DATA_DIR}/{sym}_summary.json")
    _write_json(out, f"{DATA_DIR}/_latest_summaries.json")

# Cloud Functions (Gen 2) HTTP entrypoint
def run_update(request):
    update_all()             # step 1: append data
    compute_and_save_summaries()  # step 2–3: MACO + forecast JSON
    return ("OK", 200)
