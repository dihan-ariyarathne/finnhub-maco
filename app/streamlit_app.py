# Streamlit dashboard: reads CSV + summary JSON from GCS, shows MACO & forecast.
import os, json, pandas as pd, streamlit as st
from google.cloud import storage
from pipeline.config import SYMBOLS, SHORT_WINDOW, LONG_WINDOW, DATA_DIR
import plotly.graph_objects as go

BUCKET = os.getenv("GCS_BUCKET")

def _blob(path):
    c = storage.Client(); return c.bucket(BUCKET).blob(path)

def load_prices(symbol):
    import io
    b = _blob(f"{DATA_DIR}/{symbol}.csv")
    return pd.read_csv(io.BytesIO(b.download_as_bytes()), parse_dates=["time"]).sort_values("time")

def load_summary(symbol):
    b = _blob(f"{DATA_DIR}/{symbol}_summary.json")
    if not b.exists(): return None
    return json.loads(b.download_as_text())

st.set_page_config(page_title="MACO Daily Dashboard", layout="wide")
st.title("Moving Average Crossover – Daily")

symbol = st.selectbox("Symbol", SYMBOLS)

df = load_prices(symbol)
# Recompute SMAs (for plotting). Daily MACO already ran in the Function.
df["SMA_S"] = df["c"].rolling(SHORT_WINDOW).mean()
df["SMA_L"] = df["c"].rolling(LONG_WINDOW).mean()

# Forecast summary from the daily job
summ = load_summary(symbol)
if summ:
    direction = "↑ Up" if summ["next_close"] and summ["next_close"] > summ["last_close"] else "↓ Down"
    st.subheader(f"Next-point forecast: {direction}  (last={summ['last_close']:.2f}, next≈{(summ['next_close'] or float('nan')):.2f})")

# Plot
fig = go.Figure()
fig.add_trace(go.Candlestick(x=df["time"], open=df["o"], high=df["h"], low=df["l"], close=df["c"], name="Price"))
fig.add_trace(go.Scatter(x=df["time"], y=df["SMA_S"], name=f"SMA {SHORT_WINDOW}"))
fig.add_trace(go.Scatter(x=df["time"], y=df["SMA_L"], name=f"SMA {LONG_WINDOW}"))
fig.update_layout(margin=dict(l=10,r=10,b=10,t=30), xaxis_rangeslider_visible=False)
st.plotly_chart(fig, use_container_width=True)

st.dataframe(df.tail(200))
