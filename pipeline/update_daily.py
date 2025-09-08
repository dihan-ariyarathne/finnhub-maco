# pipeline/update_daily.py
# Purpose: append new daily candles to per-symbol CSVs in GCS by
#          read -> merge -> dedupe -> rewrite (atomic with preconditions).

import io
import time
import datetime as dt
import pandas as pd
from google.cloud import storage

from pipeline.config import SYMBOLS, GCS_BUCKET, DATA_PREFIX, RESOLUTION
from pipeline.finnhub_client import candles, self_test, TOKEN

_client = storage.Client()

def _blob(relpath: str):
    return _client.bucket(GCS_BUCKET).blob(relpath)

def _read_csv(symbol: str) -> pd.DataFrame:
    """Download existing CSV and parse 'time' as datetime (UTC-naive)."""
    path = f"{DATA_PREFIX}/{symbol}.csv"
    b = _blob(path)
    if not b.exists(_client):
        # New symbol: return empty with correct schema
        return pd.DataFrame(columns=["time", "o", "h", "l", "c", "v"])
    raw = b.download_as_bytes()
    df = pd.read_csv(io.BytesIO(raw), parse_dates=["time"])
    return df

def _write_csv(symbol: str, df: pd.DataFrame):
    """Atomic rewrite with generation precondition + clean newlines."""
    path = f"{DATA_PREFIX}/{symbol}.csv"
    b = _blob(path)

    # Precondition: only write if object hasn't changed (prevents race overwrites)
    try:
        b.reload(retry=None)
        if_generation_match = int(b.generation) if b.generation is not None else 0
    except Exception:
        if_generation_match = 0

    # Force Unix newlines to avoid blank lines on Windows/Excel
    csv_bytes = df.to_csv(index=False, lineterminator="\n").encode("utf-8")

    b.upload_from_file(
        io.BytesIO(csv_bytes),
        content_type="text/csv; charset=utf-8",
        if_generation_match=if_generation_match,
    )

def _last_unix(df: pd.DataFrame) -> int:
    """Exclusive lower bound for Finnhub (seconds since epoch)."""
    if df.empty:
        # Backfill a year for brand-new symbols
        start = int((dt.datetime.utcnow() - dt.timedelta(days=365)).timestamp())
        return start - 1
    last = pd.to_datetime(df["time"].max())
    return int(last.replace(tzinfo=dt.timezone.utc).timestamp())

def _now_unix() -> int:
    """Use current UTC seconds (avoid asking the future)."""
    return int(time.time())

def _fetch_finnhub(symbol: str, fr: int, to: int) -> pd.DataFrame:
    """Call Finnhub and return normalized DF (time,o,h,l,c,v) with full bars."""
    data = candles(symbol, RESOLUTION, fr, to)  # header-auth inside
    # Finnhub returns arrays: t,o,h,l,c,v and status 's'
    if not data or data.get("s") != "ok" or not data.get("t"):
        return pd.DataFrame(columns=["time", "o", "h", "l", "c", "v"])
    df = pd.DataFrame({
        "time": pd.to_datetime(pd.Series(data["t"]), unit="s", utc=True).dt.tz_localize(None),
        "o": data["o"], "h": data["h"], "l": data["l"], "c": data["c"], "v": data["v"],
    })
    # Keep only full bars (sometimes partials come through)
    df = df.dropna(subset=["c"])
    return df

def update_symbol(symbol: str) -> dict:
    """Append latest D bars for a symbol; return counts for logging."""
    # 1) Read current CSV
    current = _read_csv(symbol)

    # 2) Time window
    fr = _last_unix(current) + 1
    to = _now_unix()

    # 3) Fetch new
    new = _fetch_finnhub(symbol, fr, to)
    if new.empty:
        return {"symbol": symbol, "added": 0, "kept": len(current)}

    # 4) Merge + dedupe + sort
    out = (pd.concat([current, new], ignore_index=True)
             .drop_duplicates(subset=["time"], keep="last")
             .sort_values("time")
             .reset_index(drop=True))

    # 5) Rewrite to GCS
    _write_csv(symbol, out)
    return {"symbol": symbol, "added": len(out) - len(current), "kept": len(out)}

def update_all() -> list:
    """Run once for all symbols. Also runs a Finnhub self-test first."""
    assert GCS_BUCKET, "GCS_BUCKET env var is required"
    # Masked token tail (helps confirm service sees the secret)
    print(f"Finnhub token (last 4): ****{(TOKEN or '')[-4:]}")
    # Self-test proves connectivity+auth from inside Cloud Run
    try:
        ping = self_test()
        print("Finnhub self-test OK:", bool(ping))
    except Exception as e:
        print("Finnhub self-test FAILED:", e)
        # Re-raise so Cloud Run returns 5xx and you notice
        raise

    results = []
    for s in SYMBOLS:
        try:
            results.append(update_symbol(s))
        except Exception as e:
            results.append({"symbol": s, "error": str(e)})
            time.sleep(0.5)  # tiny backoff between failures
    return results

if __name__ == "__main__":
    # Local Windows test (set envs first):
    #   set GCS_BUCKET=your-bucket
    #   set DATA_PREFIX=data/raw
    #   set FINNHUB_API_KEY=your-token
    #   python pipeline\update_daily.py
    print(update_all())
