-- 1) Dataset
CREATE SCHEMA IF NOT EXISTS `@project_id.@dataset`;

-- 2) External table over ALL CSVs (wildcard)
--    NOTE: define columns explicitly instead of autodetect.
CREATE OR REPLACE EXTERNAL TABLE `@project_id.@dataset.prices_ext` (
  time STRING,         -- CSV header must match: time,o,h,l,c,v
  o    FLOAT64,
  h    FLOAT64,
  l    FLOAT64,
  c    FLOAT64,
  v    FLOAT64
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://@bucket/data/raw/*.csv'],
  skip_leading_rows = 1
);

-- 3) Normalized view with symbol + proper types
CREATE OR REPLACE VIEW `@project_id.@dataset.prices` AS
SELECT
  -- robust timestamp parser: try "YYYY-MM-DD HH:MM:SS", else "YYYY-MM-DD"
  COALESCE(
    SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(time AS STRING)),
    TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', CAST(time AS STRING)))
  ) AS ts,
  CAST(o AS FLOAT64) AS o,
  CAST(h AS FLOAT64) AS h,
  CAST(l AS FLOAT64) AS l,
  CAST(c AS FLOAT64) AS c,
  CAST(v AS FLOAT64) AS v,
  REGEXP_EXTRACT(_FILE_NAME, r'/data/raw/([^/]+)\.csv$') AS symbol
FROM `YOUR_PROJECT.finnhub_data.prices_ext`
WHERE time IS NOT NULL
  -- drop rows we failed to parse to a timestamp
  AND COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CAST(time AS STRING)),
        TIMESTAMP(SAFE.PARSE_DATE('%Y-%m-%d', CAST(time AS STRING)))
      ) IS NOT NULL;

-- 4) MACO view (SMA 20/50 + cross flags)
CREATE OR REPLACE VIEW `@project_id.@dataset.maco_view` AS
WITH base AS (
  SELECT
    symbol, ts, o, h, l, c, v,
    AVG(c) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_s,
    AVG(c) OVER (PARTITION BY symbol ORDER BY ts ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS sma_l
  FROM `@project_id.@dataset.prices`
)
SELECT
  *,
  (sma_s > sma_l)
  AND (LAG(sma_s) OVER (PARTITION BY symbol ORDER BY ts) <= LAG(sma_l) OVER (PARTITION BY symbol ORDER BY ts)) AS buy_signal,
  (sma_s < sma_l)
  AND (LAG(sma_s) OVER (PARTITION BY symbol ORDER BY ts) >= LAG(sma_l) OVER (PARTITION BY symbol ORDER BY ts)) AS sell_signal
FROM base;
