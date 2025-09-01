# Finnhub REST wrapper with retries (Windows safe).
import os, time, requests
API_KEY = os.getenv("FINNHUB_API_KEY")  # set via Secret Manager in cloud

BASE = "https://finnhub.io/api/v1"

def _get(url, params, retries=3, backoff=1.5):
    params["token"] = API_KEY
    for i in range(retries):
        r = requests.get(url, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        time.sleep(backoff ** (i+1))
    r.raise_for_status()

def candles(symbol: str, resolution: str, _from: int, _to: int):
    """Fetch OHLC candles (UNIX seconds). Uses crypto endpoint for -USD symbols."""
    url = f"{BASE}/stock/candle"
    if "-USD" in symbol:
        url = f"{BASE}/crypto/candle"
        sym = f"BINANCE:{symbol.replace('-','')}T"  # e.g. BTC-USD -> BINANCE:BTCUSD
        params = {"symbol": sym, "resolution": resolution, "from": _from, "to": _to}
    else:
        params = {"symbol": symbol, "resolution": resolution, "from": _from, "to": _to}
    return _get(url, params)
