-- Replace these before running:
--   @project_id     : your project id (e.g., finnhubmacostrategy)
--   @dataset        : dataset name (e.g., finnhub_analytics)
--   @bucket         : your bucket (e.g., finnhub-mac-data-130701)

-- 1) Dataset
CREATE SCHEMA IF NOT EXISTS `@project_id.@dataset`;

-- 2) External table over ALL CSVs (wildcard)
CREATE OR REPLACE EXTERNAL TABLE `@project_id.@dataset.prices_ext`
OPTIONS (
  format = 'CSV',
  uris = ['gs://@bucket/data/raw/*.csv'],
  skip_leading_rows = 1,
  autodetect = TRUE
);

-- NOTE: CSVs don't contain the symbol, so we parse it from filename.
-- _FILE_NAME pseudo-column is available on GCS external tables.

-- 3) Normalized view with symbol + types
CREATE OR REPLACE VIEW `@project_id.@dataset.prices`
AS
SELECT
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(time AS STRING)) AS ts,
  CAST(o AS FLOAT64) AS o,
  CAST(h AS FLOAT64) AS h,
  CAST(l AS FLOAT64) AS l,
  CAST(c AS FLOAT64) AS c,
  CAST(v AS FLOAT64) AS v,
  REGEXP_EXTRACT(_FILE_NAME, r'/data/raw/([^/]+)\\.csv$') AS symbol
FROM `@project_id.@dataset.prices_ext`
WHERE time IS NOT NULL;

-- 4) MACO view (SMA + cross)
CREATE OR REPLACE VIEW `@project_id.@dataset.maco_view`
AS
WITH base AS (
  SELECT
    symbol,
    ts,
    o,h,l,c,v,
    -- Adjust windows here if needed:
    AVG(c) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_s,  -- 20
    AVG(c) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_l   -- 50
  FROM `@project_id.@dataset.prices`
)
SELECT
  *,
  -- bullish cross today if sma_s > sma_l and yesterday sma_s <= sma_l
  (sma_s > sma_l) AND (LAG(sma_s) OVER (PARTITION BY symbol ORDER BY ts) <= LAG(sma_l) OVER (PARTITION BY symbol ORDER BY ts)) AS buy_signal,
  (sma_s < sma_l) AND (LAG(sma_s) OVER (PARTITION BY symbol ORDER BY ts) >= LAG(sma_l) OVER (PARTITION BY symbol ORDER BY ts)) AS sell_signal
FROM base;
