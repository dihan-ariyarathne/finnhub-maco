# Appends the latest daily candles from Finnhub to the CSVs in GCS
# Columns remain: time,o,h,l,c,v  (so BigQuery/Looker keep working)

import os, io, time, datetime as dt
import pandas as pd
from google.cloud import storage
from pipeline.config import SYMBOLS                  # your tickers
from pipeline.finnhub_client import candles         # your existing helper

# --- Config (env overrides keep your structure) ---
BUCKET = os.getenv("GCS_BUCKET")                    # required
DATA_PREFIX = os.getenv("DATA_PREFIX", "data/raw")  # where CSVs live
RESOLUTION = "D"                                    # daily candles

_client = storage.Client()

def _blob(relpath: str):
    return _client.bucket(BUCKET).blob(relpath)

def _read_csv(symbol: str) -> pd.DataFrame:
    """Download existing CSV from GCS and parse 'time' as datetime."""
    b = _blob(f"{DATA_PREFIX}/{symbol}.csv")
    if not b.exists():
        # If file missing (new symbol), return empty frame with correct schema
        return pd.DataFrame(columns=["time", "o", "h", "l", "c", "v"])
    raw = b.download_as_bytes()
    df = pd.read_csv(io.BytesIO(raw), parse_dates=["time"])
    return df

def _write_csv(symbol: str, df: pd.DataFrame):
    """Upload CSV back to GCS (no index)."""
    b = _blob(f"{DATA_PREFIX}/{symbol}.csv")
    buf = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
    b.upload_from_file(buf, content_type="text/csv")

def _last_unix(df: pd.DataFrame) -> int:
    """Get last timestamp in seconds (exclusive lower bound for Finnhub)."""
    if df.empty:
        # backstop: start 30 days ago (daily limit-friendly)
        start = int((dt.datetime.utcnow() - dt.timedelta(days=30)).timestamp())
        return start - 1
    last = pd.to_datetime(df["time"].max())
    return int(last.replace(tzinfo=dt.timezone.utc).timestamp())

def _now_unix() -> int:
    # use future +1d to ensure we catch today's bar when available
    return int((dt.datetime.utcnow() + dt.timedelta(days=1)).timestamp())

def _fetch_finnhub(symbol: str, fr: int, to: int) -> pd.DataFrame:
    """Call Finnhub /stock/candle and return normalized DF (time,o,h,l,c,v)."""
    data = candles(symbol, RESOLUTION, fr, to)  # uses your finnhub_client.py
    # Finnhub returns arrays: t,o,h,l,c,v with status 's'
    if not data or data.get("s") != "ok" or not data.get("t"):
        return pd.DataFrame(columns=["time", "o", "h", "l", "c", "v"])
    df = pd.DataFrame({
        "time": pd.to_datetime(pd.Series(data["t"]), unit="s", utc=True).dt.tz_localize(None),
        "o": data["o"], "h": data["h"], "l": data["l"], "c": data["c"], "v": data["v"],
    })
    # Keep only daily “full bars” (Finnhub may return partial)
    df = df.dropna(subset=["c"])
    return df

def update_symbol(symbol: str) -> dict:
    """Append latest daily bars for one symbol; return counts for logging."""
    current = _read_csv(symbol)
    fr = _last_unix(current) + 1
    to = _now_unix()
    new = _fetch_finnhub(symbol, fr, to)

    if new.empty:
        return {"symbol": symbol, "added": 0, "kept": len(current)}

    # concat, de-dup on 'time', sort
    out = (pd.concat([current, new], ignore_index=True)
             .drop_duplicates(subset=["time"])
             .sort_values("time"))

    _write_csv(symbol, out)
    return {"symbol": symbol, "added": len(out) - len(current), "kept": len(out)}

def update_all() -> list:
    """Run append for all configured symbols."""
    assert BUCKET, "GCS_BUCKET env var is required"
    results = []
    for s in SYMBOLS:
        try:
            results.append(update_symbol(s))
        except Exception as e:
            results.append({"symbol": s, "error": str(e)})
            time.sleep(0.5)  # tiny backoff between failures
    return results

if __name__ == "__main__":
    # Local/Windows test:
    # set GCS_BUCKET=your-bucket && set FINNHUB_API_KEY=your-key && python pipeline\update_daily.py
    print(update_all())
