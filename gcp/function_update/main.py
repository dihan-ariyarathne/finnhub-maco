# Streamlit dashboard: reads CSV + summary JSON from GCS, shows MACO & forecast.

import os, io, json
import pandas as pd
import streamlit as st
from google.cloud import storage
from pipeline.config import SYMBOLS, SHORT_WINDOW, LONG_WINDOW
import plotly.graph_objects as go

# ADJUSTED: read bucket from env; fail fast if missing
BUCKET = os.getenv("GCS_BUCKET")
if not BUCKET:
    st.error("GCS_BUCKET env var is not set. Set it on Cloud Run.")
    st.stop()

# ADJUSTED: replace DATA_DIR import with a prefix that points inside the bucket
DATA_PREFIX = os.getenv("DATA_PREFIX", "data/raw")  # e.g., gs://bucket/data/raw/*.csv

# ADJUSTED: create a single storage client (faster) + tiny helper
_client = storage.Client()

def _blob(path: str):
    return _client.bucket(BUCKET).blob(path)

# ADJUSTED: cache downloads; load CSV from GCS; auto-handle epoch timestamps
@st.cache_data(ttl=300)
def load_prices(symbol: str) -> pd.DataFrame:
    b = _blob(f"{DATA_PREFIX}/{symbol}.csv")
    if not b.exists():
        raise FileNotFoundError(f"Missing {symbol}.csv in gs://{BUCKET}/{DATA_PREFIX}")
    df = pd.read_csv(io.BytesIO(b.download_as_bytes()))
    # Ensure we have a datetime index/column named 'time'
    if "time" in df.columns:
        if pd.api.types.is_integer_dtype(df["time"]) or pd.api.types.is_float_dtype(df["time"]):
            # epoch seconds -> timestamp
            df["time"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_localize(None)
        else:
            df["time"] = pd.to_datetime(df["time"])
    else:
        # If no 'time' column, try to infer from index
        df.reset_index(inplace=True)
        df.rename(columns={"index": "time"}, inplace=True)
        df["time"] = pd.to_datetime(df["time"])
    df = df.sort_values("time")
    return df

# ADJUSTED: load optional prediction summary JSON from the same GCS prefix
@st.cache_data(ttl=300)
def load_summary(symbol: str):
    b = _blob(f"{DATA_PREFIX}/{symbol}_summary.json")
    if not b.exists():
        return None
    return json.loads(b.download_as_text())

st.set_page_config(page_title="MACO Daily Dashboard", layout="wide")
st.title("Moving Average Crossover – Daily")

symbol = st.selectbox("Symbol", SYMBOLS)

# ADJUSTED: guard for missing files with a friendly message
try:
    df = load_prices(symbol)
except FileNotFoundError as e:
    st.warning(str(e))
    st.stop()

# Recompute SMAs (for plotting). Daily MACO already ran in the Function.
df["SMA_S"] = df["c"].rolling(SHORT_WINDOW).mean()
df["SMA_L"] = df["c"].rolling(LONG_WINDOW).mean()

# Forecast summary from the daily job
summ = load_summary(symbol)
if summ and "last_close" in summ and "next_close" in summ:
    try:
        direction = "↑ Up" if (summ["next_close"] is not None and summ["next_close"] > summ["last_close"]) else "↓ Down"
        st.subheader(
            f"Next-point forecast: {direction}  "
            f"(last={summ['last_close']:.2f}, next≈{(summ['next_close'] or float('nan')):.2f})"
        )
    except Exception:
        pass  # keep UI resilient if fields are missing

# Plot
fig = go.Figure()
fig.add_trace(go.Candlestick(x=df["time"], open=df["o"], high=df["h"], low=df["l"], close=df["c"], name="Price"))
fig.add_trace(go.Scatter(x=df["time"], y=df["SMA_S"], name=f"SMA {SHORT_WINDOW}"))
fig.add_trace(go.Scatter(x=df["time"], y=df["SMA_L"], name=f"SMA {LONG_WINDOW}"))
fig.update_layout(margin=dict(l=10, r=10, b=10, t=30), xaxis_rangeslider_visible=False)
st.plotly_chart(fig, use_container_width=True)

st.dataframe(df.tail(200))
