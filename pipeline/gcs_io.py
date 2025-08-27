# pipeline/gcs_io.py
# Purpose: small helpers to read/write CSV & JSON in your GCS bucket.

import os, io, json, pandas as pd
from google.cloud import storage

BUCKET = os.getenv("GCS_BUCKET")               # set in env / Actions / Cloud Run
PREFIX = os.getenv("DATA_PREFIX", "data/raw")  # path inside the bucket

_client = storage.Client()                     # create GCS client (reused)
_bucket = _client.bucket(BUCKET)               # pointer to your bucket

def _blob(path: str):
    """Return a handle to gs://<bucket>/<path>"""  # Windows: helper only
    return _bucket.blob(path)

def read_csv(symbol: str) -> pd.DataFrame:
    """Read <symbol>.csv from GCS; empty DataFrame if missing."""
    b = _blob(f"{PREFIX}/{symbol}.csv")
    if not b.exists():
        return pd.DataFrame(columns=["time","o","h","l","c","v"])
    df = pd.read_csv(io.BytesIO(b.download_as_bytes()))
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce").dt.tz_localize(None)
    return df

def write_csv(symbol: str, df: pd.DataFrame):
    """Write DataFrame to <symbol>.csv in GCS."""
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    _blob(f"{PREFIX}/{symbol}.csv").upload_from_string(csv_bytes, content_type="text/csv")

def write_summary(symbol: str, payload: dict):
    """Write <symbol>_summary.json to GCS (dashboard/metrics)."""
    _blob(f"{PREFIX}/{symbol}_summary.json").upload_from_string(
        json.dumps(payload), content_type="application/json"
    )
