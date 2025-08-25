# Seeds historical data once into GCS.
# Run locally or in GitHub Action (seed.yml). On cloud, FINNHUB_API_KEY comes from Secret Manager.
import os, time, pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from pipeline.config import SYMBOLS, RESOLUTION, HIST_YEARS, DATA_DIR
from pipeline.finnhub_client import candles

BUCKET = os.getenv("GCS_BUCKET")  # set in CI/cloud

def _write_csv(df, path):
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(path)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")

def seed_symbol(symbol: str):
    end = int(time.time())
    start = int((datetime.utcnow() - timedelta(days=365*HIST_YEARS)).timestamp())
    data = candles(symbol, RESOLUTION, start, end)
    if data.get("s") != "ok":
        print(f"[WARN] No data for {symbol}"); return
    df = pd.DataFrame({"t": data["t"], "o": data["o"], "h": data["h"], "l": data["l"], "c": data["c"], "v": data["v"]})
    df["time"] = pd.to_datetime(df["t"], unit="s", utc=True).dt.tz_convert("UTC")
    df = df.drop(columns=["t"]).sort_values("time")
    _write_csv(df, f"{DATA_DIR}/{symbol}.csv")
    print(f"[OK] Seeded {symbol}: {len(df)} rows")

if __name__ == "__main__":
    for s in SYMBOLS: seed_symbol(s)
