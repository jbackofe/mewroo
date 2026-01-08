CREATE TABLE IF NOT EXISTS stock_prices (
  `Date` DateTime64(0, 'UTC'),
  `Open` Float64,
  `High` Float64,
  `Low` Float64,
  `Close` Float64,
  `Volume` UInt64,
  `Dividends` Float64,
  `Stock Splits` Float64,
  `Company` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(Date)
ORDER BY (Company, Date);
