# Holds knobs shared by app + jobs.
# pipeline/config.py
# Purpose: central config (symbols, GCS paths, resolution).
# Windows-friendly: reads ENV so you can set vars in PowerShell/cmd.

import os

# Symbols you track (stocks plain, crypto as BINANCE:PAIR)
# Tip: If you prefer BTC-USD, we map it in finnhub_client.py.
SYMBOLS = os.getenv("SYMBOLS", "AAPL,TSLA,BINANCE:BTCUSDT").split(",")

# GCS bucket and prefix where CSVs live
GCS_BUCKET = os.getenv("GCS_BUCKET")                 # REQUIRED at runtime
DATA_PREFIX = os.getenv("DATA_PREFIX", "data/raw")   # default folder

# Candle resolution (D = daily)
RESOLUTION = os.getenv("RESOLUTION", "D")

HIST_YEARS = 5                         # how far back to seed
SHORT_WINDOW = 20                      # MACO short SMA
LONG_WINDOW = 50                       # MACO long SMA