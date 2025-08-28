# Reads recent rows from each CSV in GCS, computes MACO, writes today's signal to BigQuery
# Windows-friendly; comments show what each section does

import os, io, datetime as dt
import pandas as pd
from google.cloud import storage, bigquery
from pipeline.config import SYMBOLS, SHORT_WINDOW, LONG_WINDOW
from pipeline.maco_compute import add_maco

# ---- Config (env-driven) ----
BUCKET      = os.getenv("GCS_BUCKET")                     # required
DATA_PREFIX = os.getenv("DATA_PREFIX", "data/raw")        # where CSVs live
BQ_PROJECT  = os.getenv("BQ_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")  # fallback
BQ_DATASET  = os.getenv("BQ_DATASET", "finnhub_data")     # dataset
BQ_TABLE    = os.getenv("BQ_TABLE",   "maco_signals")     # table name

if not BUCKET:     raise RuntimeError("GCS_BUCKET is required")
if not BQ_PROJECT: raise RuntimeError("BQ_PROJECT/GOOGLE_CLOUD_PROJECT is required")

_storage = storage.Client()
_bq      = bigquery.Client(project=BQ_PROJECT)

def _blob(path: str):
    return _storage.bucket(BUCKET).blob(path)

def _read_recent(symbol: str, lookback_rows: int = 400) -> pd.DataFrame:
    """Read last N rows from the CSV for stable SMA, normalize types."""
    b = _blob(f"{DATA_PREFIX}/{symbol}.csv")
    if not b.exists():
        return pd.DataFrame(columns=["time","o","h","l","c","v"])
    raw = b.download_as_bytes()
    df = pd.read_csv(io.BytesIO(raw), parse_dates=["time"]).sort_values("time")
    return df.tail(lookback_rows).copy()

def _ensure_table():
    """Create BigQuery table if missing (partitioned by date)."""
    dataset_ref = bigquery.Dataset(f"{BQ_PROJECT}.{BQ_DATASET}")
    try:
        _bq.get_dataset(dataset_ref)
    except Exception:
        _bq.create_dataset(dataset_ref, exists_ok=True)

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    schema = [
        bigquery.SchemaField("symbol",      "STRING"),
        bigquery.SchemaField("ts",          "TIMESTAMP"),
        bigquery.SchemaField("close",       "FLOAT"),
        bigquery.SchemaField("sma_s",       "FLOAT"),
        bigquery.SchemaField("sma_l",       "FLOAT"),
        bigquery.SchemaField("buy_signal",  "BOOL"),
        bigquery.SchemaField("sell_signal", "BOOL"),
        bigquery.SchemaField("direction",   "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ts",
    )
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = time_partitioning
    _bq.create_table(table, exists_ok=True)

def publish_for_symbol(symbol: str) -> dict:
    df = _read_recent(symbol)
    if df.empty:
        return {"symbol": symbol, "status": "no_data"}

    # MACO on recent window using your config windows
    maco = add_maco(df, SHORT_WINDOW, LONG_WINDOW)

    # take the latest row only (today’s signal)
    last = maco.iloc[-1]
    out = pd.DataFrame([{
        "symbol":      symbol,
        "ts":          pd.to_datetime(last["time"]),
        "close":       float(last["c"]),
        "sma_s":       float(last["sma_s"]) if pd.notna(last["sma_s"]) else None,
        "sma_l":       float(last["sma_l"]) if pd.notna(last["sma_l"]) else None,
        "buy_signal":  bool(last["buy_signal"]) if pd.notna(last["buy_signal"]) else False,
        "sell_signal": bool(last["sell_signal"]) if pd.notna(last["sell_signal"]) else False,
        "direction":   str(last["direction"]) if pd.notna(last["direction"]) else "Hold",
        "ingested_at": dt.datetime.utcnow(),
    }])

    _ensure_table()

    job = _bq.load_table_from_dataframe(
        out,
        f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    return {"symbol": symbol, "status": "ok"}

def publish_all() -> list:
    results = []
    for s in SYMBOLS:
        try:
            results.append(publish_for_symbol(s))
        except Exception as e:
            results.append({"symbol": s, "status": f"error: {e}"})
    return results

if __name__ == "__main__":
    # Local Windows test:
    # set GCS_BUCKET=your-bucket && set BQ_PROJECT=your-project && python pipeline\publish_signals.py
    print(publish_all())
