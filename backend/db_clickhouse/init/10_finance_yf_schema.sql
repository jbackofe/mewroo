CREATE DATABASE IF NOT EXISTS finance_yf;

CREATE TABLE IF NOT EXISTS finance_yf.dim_industry (
  sector_key LowCardinality(String),
  industry_key LowCardinality(String),
  industry_name String,
  industry_symbol LowCardinality(String),
  market_weight Float64,
  asof_date DateTime64(0, 'UTC'),
  ingested_at DateTime64(0, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (sector_key, industry_key, asof_date);

CREATE TABLE IF NOT EXISTS finance_yf.industry_membership (
  asof_date DateTime64(0, 'UTC'),
  sector_key LowCardinality(String),
  industry_key LowCardinality(String),
  ticker LowCardinality(String),
  ticker_name String,
  source LowCardinality(String),
  ingested_at DateTime64(0, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (industry_key, asof_date, ticker, source);

CREATE TABLE IF NOT EXISTS finance_yf.dim_ticker (
  asof_date DateTime64(0, 'UTC'),
  ticker LowCardinality(String),
  long_name String,
  short_name String,
  exchange LowCardinality(String),
  currency LowCardinality(String),
  country LowCardinality(String),
  sector String,
  industry String,
  quote_type LowCardinality(String),
  timezone LowCardinality(String),
  website String,
  ingested_at DateTime64(0, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, asof_date);

CREATE TABLE IF NOT EXISTS finance_yf.fact_stock_prices (
  ts DateTime64(0, 'UTC'),
  ticker LowCardinality(String),
  interval LowCardinality(String),
  open Float64,
  high Float64,
  low Float64,
  close Float64,
  adj_close Float64,
  volume UInt64,
  source LowCardinality(String) DEFAULT 'yfinance',
  ingested_at DateTime64(0, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (ticker, interval, ts);

CREATE TABLE IF NOT EXISTS finance_yf.fact_market_cap (
  asof_date DateTime64(0, 'UTC'),
  ticker LowCardinality(String),
  market_cap Float64,
  currency LowCardinality(String),
  source LowCardinality(String) DEFAULT 'yfinance_info',
  ingested_at DateTime64(0, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (ticker, asof_date);

CREATE TABLE finance_yf.ingest_state (
  source LowCardinality(String),
  target LowCardinality(String),
  key LowCardinality(String),
  last_ts Nullable(DateTime64(0, 'UTC')),
  last_asof_date Nullable(DateTime64(0, 'UTC')),
  updated_at DateTime64(0, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source, target, key);
