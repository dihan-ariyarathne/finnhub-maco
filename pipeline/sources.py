# pipeline/sources.py
# Purpose: download daily OHLCV from Yahoo Finance and normalize columns.

import pandas as pd, yfinance as yf

def yf_daily(symbol: str, period="2y", start=None, end=None) -> pd.DataFrame:
    """
    Get daily OHLCV from Yahoo.
    - Default ~2 years via period="2y".
    - If start/end given, uses them.
    Returns columns: time,o,h,l,c,v (UTC-naive).
    """
    if start or end:
        df = yf.download(symbol, start=start, end=end, interval="1d",
                         auto_adjust=False, progress=False)
    else:
        df = yf.download(symbol, period=period, interval="1d",
                         auto_adjust=False, progress=False)

    if df.empty:
        return pd.DataFrame(columns=["time","o","h","l","c","v"])

    df = df.rename(columns={"Open":"o","High":"h","Low":"l","Close":"c","Volume":"v"})
    df.index.name = "time"
    df.reset_index(inplace=True)
    df["time"] = pd.to_datetime(df["time"]).dt.tz_localize(None)  # Windows: normalize timestamp
    return df[["time","o","h","l","c","v"]]
