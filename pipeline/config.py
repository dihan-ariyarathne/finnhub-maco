# Holds knobs shared by app + jobs.
SYMBOLS = ["AAPL", "TSLA", "BTC-USD"]  # as in your plan
RESOLUTION = "D"                       # daily candles
HIST_YEARS = 5                         # how far back to seed
SHORT_WINDOW = 20                      # MACO short SMA
LONG_WINDOW = 50                       # MACO long SMA
DATA_DIR = "data/raw"                  # path prefix inside GCS bucket