#!/bin/sh
set -e

CSV_PATH="/data/stock_details_5_years.csv"

if [ ! -f "$CSV_PATH" ]; then
  echo "Seed CSV not found at $CSV_PATH. Skipping import."
  exit 0
fi

echo "Checking whether stock_prices already has data..."
ROWS=$(clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT count() FROM stock_prices")

if [ "$ROWS" -gt 0 ]; then
  echo "stock_prices already has $ROWS rows. Skipping import."
  exit 0
fi

echo "Importing $CSV_PATH into stock_prices..."
clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "
    INSERT INTO stock_prices
    SELECT
      toTimeZone(parseDateTimeBestEffort(Date), 'UTC') AS Date,
      Open, High, Low, Close,
      toUInt64(Volume) AS Volume,
      Dividends,
      \"Stock Splits\" AS \"Stock Splits\",
      Company
    FROM input(
      'Date String, Open Float64, High Float64, Low Float64, Close Float64,
       Volume Float64, Dividends Float64, \"Stock Splits\" Float64, Company String'
    )
    FORMAT CSVWithNames
  " < "$CSV_PATH"

echo "Import complete."
