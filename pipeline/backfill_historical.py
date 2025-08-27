# pipeline/backfill_historical.py
# Purpose: pull ~2 years from Yahoo and save to GCS once (no Finnhub here).

from pipeline.config import SYMBOLS
from pipeline.sources import yf_daily
from pipeline.gcs_io import write_csv

def backfill_symbol(symbol: str):
    """Fetch ~2y from Yahoo and write <symbol>.csv to GCS."""
    df = yf_daily(symbol, period="2y")                      # <-- Yahoo ONLY
    df = df.drop_duplicates(subset=["time"]).sort_values("time")
    write_csv(symbol, df)
    print(f"[{symbol}] backfilled {len(df):,} rows")

if __name__ == "__main__":
    for s in SYMBOLS:
        backfill_symbol(s)
