# Computes SMA crossover signals on a price DataFrame
# Expects columns: time (datetime), c (close)
import pandas as pd

def add_maco(df: pd.DataFrame, short_win: int, long_win: int) -> pd.DataFrame:
    # compute short/long SMAs
    df = df.sort_values("time").copy()
    df["sma_s"] = df["c"].rolling(short_win).mean()   # short SMA
    df["sma_l"] = df["c"].rolling(long_win).mean()    # long SMA

    # crossover: buy when short crosses above long; sell when crosses below
    prev_s = df["sma_s"].shift(1)
    prev_l = df["sma_l"].shift(1)
    df["buy_signal"]  = (df["sma_s"] > df["sma_l"]) & (prev_s <= prev_l)
    df["sell_signal"] = (df["sma_s"] < df["sma_l"]) & (prev_s >= prev_l)

    # optional text direction (for dashboards)
    df["direction"] = df.apply(
        lambda r: "Buy" if r["buy_signal"] else ("Sell" if r["sell_signal"] else "Hold"),
        axis=1
    )
    return df