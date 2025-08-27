# pipeline/update_daily.py
# Purpose: append NEW rows daily using Finnhub ONLY, then write a small summary JSON.

import os, pandas as pd, requests
from datetime import datetime, timedelta, timezone
from pipeline.config import SYMBOLS, SHORT_WINDOW, LONG_WINDOW
from pipeline.gcs_io import read_csv, write_csv, write_summary

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")   # provided via Secret Manager in deploy

def _epoch(dt: datetime) -> int:
    """Convert datetime -> epoch seconds (UTC)."""
    return int((dt - datetime(1970,1,1)).total_seconds())

def _fetch_finnhub_candles(symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """Call Finnhub /stock/candle (D) and return normalized DataFrame with time,o,h,l,c,v."""
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY missing")

    url = "https://finnhub.io/api/v1/stock/candle"
    params = {
        "symbol": symbol,
        "resolution": "D",
        "from": _epoch(start_dt),
        "to":   _epoch(end_dt),
        "token": FINNHUB_API_KEY,
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    if j.get("s") != "ok":
        # Fail hard so we don't silently mix sources
        raise RuntimeError(f"Finnhub error for {symbol}: {j}")

    df = pd.DataFrame({"time": j["t"], "o": j["o"], "h": j["h"], "l": j["l"], "c": j["c"], "v": j["v"]})
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_localize(None)
    return df[["time","o","h","l","c","v"]]

def _forecast_next_close(df: pd.DataFrame):
    """Simple next-close forecast: last short SMA value (for display)."""
    if df.empty: return None
    s = df["c"].rolling(SHORT_WINDOW).mean().iloc[-1]
    return float(s) if pd.notna(s) else None

def update_symbol(symbol: str) -> int:
    """Append new rows for one symbol using FINNHUB ONLY. Return #rows appended."""
    existing = read_csv(symbol)                                    # read current CSV from GCS
    last_ts = existing["time"].max() if not existing.empty else None

    # compute fetch window [last+1day .. now+1day] to include today if available
    start_dt = (last_ts + timedelta(days=1)) if last_ts is not None else datetime(2000,1,1)
    end_dt = datetime.utcnow().date() + timedelta(days=1)

    # fetch candles strictly from Finnhub
    new_df = _fetch_finnhub_candles(symbol, start_dt, datetime.combine(end_dt, datetime.min.time()))

    if new_df.empty:
        print(f"[{symbol}] no new rows from Finnhub")
        return 0

    all_df = pd.concat([existing, new_df], ignore_index=True)
    all_df = all_df.drop_duplicates(subset=["time"]).sort_values("time")

    write_csv(symbol, all_df)                                      # write merged CSV back to GCS

    # write a tiny summary JSON for the dashboard
    last_close = float(all_df["c"].iloc[-1])
    next_close = _forecast_next_close(all_df)
    write_summary(symbol, {
        "symbol": symbol,
        "last_close": last_close,
        "next_close": next_close,
        "asof": datetime.now(timezone.utc).isoformat()
    })

    appended = len(all_df) - len(existing)
    print(f"[{symbol}] appended {appended} rows (total {len(all_df)})")
    return appended

def update_all(symbol: str | None = None):
    """Entry point for Cloud Function: update one or all symbols (Finnhub only)."""
    targets = [symbol] if symbol else SYMBOLS
    updated = {}
    for s in targets:
        try:
            updated[s] = update_symbol(s)
        except Exception as e:
            updated[s] = f"error: {e}"
    return {"updated": updated}
