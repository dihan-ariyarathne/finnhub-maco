# Backfills ~2 years of daily OHLCV from Yahoo Finance to GCS.
# Windows-friendly; comments explain each step.

import os
import io
import pandas as pd
import yfinance as yf
from google.cloud import storage
from pipeline.config import SYMBOLS  # your list of tickers

# ---- Config from env (kept same as the rest of your repo) ----
BUCKET = os.getenv("GCS_BUCKET")  # must be set by GH Action
DATA_PREFIX = os.getenv("DATA_PREFIX", "data/raw")  # keep same path used by BQ

if not BUCKET:
    raise RuntimeError("GCS_BUCKET env var is required for backfill.")

_storage = storage.Client()  # one client for speed


def fetch_yahoo_2y(symbol: str) -> pd.DataFrame:
    """Download ~2 years of daily candles from Yahoo and normalize columns."""
    # Get daily data; no auto adjustments so Close stays 'c'
    df = yf.download(
        symbol, period="2y", interval="1d",
        auto_adjust=False, actions=False, progress=False
    ).reset_index()  # Date becomes a column

    if df.empty:
        raise RuntimeError(f"Yahoo returned no data for {symbol}")

    # Rename to our schema expected by the rest of the pipeline/BQ:
    # time,o,h,l,c,v
    rename_map = {
        "Date": "time",
        "Open": "o",
        "High": "h",
        "Low": "l",
        "Close": "c",
        "Volume": "v",
    }
    df = df.rename(columns=rename_map)

    # Keep only required columns (some tickers have 'Adj Close', drop it)
    df = df[["time", "o", "h", "l", "c", "v"]]

    # Ensure proper dtypes and a tz-naive timestamp for portability
    df["time"] = pd.to_datetime(df["time"], utc=False).dt.tz_localize(None)
    for col in ["o", "h", "l", "c", "v"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove any bad rows, de-duplicate on time, sort ascending
    df = df.dropna(subset=["c"]).drop_duplicates(subset=["time"]).sort_values("time")

    return df


def write_csv_to_gcs(df: pd.DataFrame, path: str) -> None:
    """Write DataFrame CSV (no index) to GCS path inside the bucket."""
    bucket = _storage.bucket(BUCKET)
    blob = bucket.blob(path)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_file(io.BytesIO(csv_bytes), content_type="text/csv")


def backfill_symbol(symbol: str) -> str:
    df = fetch_yahoo_2y(symbol)
    dest = f"{DATA_PREFIX}/{symbol}.csv"
    write_csv_to_gcs(df, dest)
    return dest


if __name__ == "__main__":
    # Run for all symbols defined in pipeline.config.SYMBOLS
    for s in SYMBOLS:
        out = backfill_symbol(s)
        print(f"Wrote: gs://{BUCKET}/{out}")
