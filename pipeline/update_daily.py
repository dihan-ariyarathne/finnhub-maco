# Appends latest candles into GCS CSVs (de-dup on timestamp).
import os, time, pandas as pd
from datetime import datetime, timedelta
from google.cloud import storage
from pipeline.config import SYMBOLS, RESOLUTION, DATA_DIR
from pipeline.finnhub_client import candles

BUCKET = os.getenv("GCS_BUCKET")

def _gcs():
    from google.cloud import storage
    return storage.Client()

def _read_csv(path):
    b = _gcs().bucket(BUCKET).blob(path)
    import io
    return pd.read_csv(io.BytesIO(b.download_as_bytes()))

def _write_csv(df, path):
    _gcs().bucket(BUCKET).blob(path).upload_from_string(df.to_csv(index=False), content_type="text/csv")

def update_symbol(symbol: str):
    path = f"{DATA_DIR}/{symbol}.csv"
    try:
        cur = _read_csv(path)
        last_ts = pd.to_datetime(cur["time"]).max()
        start = int((last_ts.to_pydatetime().replace(tzinfo=None) - timedelta(days=3)).timestamp())
    except Exception:
        cur = pd.DataFrame(); start = int((datetime.utcnow() - timedelta(days=365)).timestamp())
    end = int(time.time())
    data = candles(symbol, RESOLUTION, start, end)
    if data.get("s") != "ok":
        print(f"[WARN] No new data for {symbol}"); return
    inc = pd.DataFrame({"t": data["t"], "o": data["o"], "h": data["h"], "l": data["l"], "c": data["c"], "v": data["v"]})
    inc["time"] = pd.to_datetime(inc["t"], unit="s", utc=True).dt.tz_convert("UTC")
    inc = inc.drop(columns=["t"]).sort_values("time")
    out = pd.concat([cur, inc], ignore_index=True).drop_duplicates(subset=["time"]).sort_values("time")
    _write_csv(out, path)
    print(f"[OK] Updated {symbol}: +{len(inc)} rows, total={len(out)}")

def update_all():
    for s in SYMBOLS: update_symbol(s)

if __name__ == "__main__":
    update_all()
