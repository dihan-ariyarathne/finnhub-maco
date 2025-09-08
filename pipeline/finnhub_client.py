# pipeline/finnhub_client.py
# Purpose: tiny Finnhub client with header auth + clear errors + self-test.

import os
import time
import requests

BASE = "https://finnhub.io/api/v1"
TOKEN = os.getenv("FINNHUB_API_KEY")  # injected via Secret Manager or env

# Optional symbol aliases (lets you keep BTC-USD in config)
ALIAS = {
    "BTC-USD": "BINANCE:BTCUSDT",
}

def _normalize_symbol(symbol: str) -> str:
    """Map friendly symbols to Finnhub format (esp. crypto)."""
    return ALIAS.get(symbol, symbol)

def _req(path: str, params: dict):
    """GET with token header + loud, helpful errors."""
    if not TOKEN:
        raise RuntimeError("FINNHUB_API_KEY is missing at runtime")
    headers = {"X-Finnhub-Token": TOKEN}  # use header auth (cleaner than query)
    r = requests.get(f"{BASE}{path}", params=params, headers=headers, timeout=20)
    if r.status_code == 403:
        # Print short body so you know *why* (invalid, plan, IP allowlist, etc.)
        raise RuntimeError(f"Finnhub 403: {r.text[:200]}")
    r.raise_for_status()
    return r.json()

def candles(symbol: str, resolution: str, fr: int, to: int) -> dict:
    """Fetch candles for stock or crypto, auto-route based on symbol format."""
    symbol = _normalize_symbol(symbol)
    if ":" in symbol:  # e.g., BINANCE:BTCUSDT means crypto
        return _req("/crypto/candle", {"symbol": symbol, "resolution": resolution, "from": fr, "to": to})
    return _req("/stock/candle", {"symbol": symbol, "resolution": resolution, "from": fr, "to": to})

def self_test() -> dict:
    """Quick health call to prove token works from inside Cloud Run."""
    return _req("/quote", {"symbol": "AAPL"})
