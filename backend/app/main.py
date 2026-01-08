import os
import time
from typing import Generator

import clickhouse_connect
from fastapi import Depends, FastAPI, Query

app = FastAPI(title="mewroo API")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_USER = os.getenv("CLICKHOUSE_USER", "mewroo")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "mewroo")


def ch_client() -> Generator:
  """
  Create a fresh ClickHouse client per request.
  clickhouse-connect clients are not safe to share across concurrent requests.
  """
  client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    database=CH_DB,
    username=CH_USER,
    password=CH_PASSWORD,
  )
  try:
    yield client
  finally:
    # clickhouse-connect exposes close() for httpclient
    try:
      client.close()
    except Exception:
      pass


@app.on_event("startup")
def wait_for_clickhouse():
  last_err = None
  for _ in range(30):
    try:
      c = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        database=CH_DB,
        username=CH_USER,
        password=CH_PASSWORD,
      )
      c.command("SELECT 1")
      try:
        c.close()
      except Exception:
        pass
      return
    except Exception as e:
      last_err = e
      time.sleep(1)
  raise RuntimeError(f"ClickHouse not ready after waiting: {last_err}")


@app.get("/api/health")
def health():
  return {"ok": True}


@app.get("/api/finance/symbols")
def symbols(limit: int = 5000, client=Depends(ch_client)):
  q = """
    SELECT DISTINCT Company
    FROM stock_prices
    ORDER BY Company
    LIMIT %(limit)s
  """
  res = client.query(q, parameters={"limit": limit})
  return {"data": [row[0] for row in res.result_rows]}


@app.get("/api/finance/meta")
def meta(symbol: str = Query(...), client=Depends(ch_client)):
  q = """
    SELECT max(Date) AS max_date, min(Date) AS min_date
    FROM stock_prices
    WHERE Company = %(symbol)s
  """
  res = client.query(q, parameters={"symbol": symbol})
  max_date, min_date = res.result_rows[0]
  return {
    "symbol": symbol,
    "min_date": min_date.isoformat() if min_date else None,
    "max_date": max_date.isoformat() if max_date else None,
  }


@app.get("/api/finance/history")
def history(
  symbol: str = Query(...),
  start: str = Query(...),
  end: str = Query(...),
  granularity: str = Query("day", pattern="^(day|week|month)$"),
  client=Depends(ch_client),
):
  if granularity == "day":
    bucket = "toDate(ts)"
  elif granularity == "week":
    bucket = "toStartOfWeek(ts)"
  else:
    bucket = "toStartOfMonth(ts)"

  q = f"""
    SELECT
      {bucket} AS bucket,
      anyLast(Close) AS close
    FROM (
      SELECT
        Date AS ts,
        Close
      FROM stock_prices
      WHERE Company = %(symbol)s
        AND Date >= parseDateTimeBestEffort(%(start)s)
        AND Date <  parseDateTimeBestEffort(%(end)s)
      ORDER BY ts
    )
    GROUP BY bucket
    ORDER BY bucket
  """

  res = client.query(q, parameters={"symbol": symbol, "start": start, "end": end})
  rows = [{"ts": r[0].isoformat(), "close": float(r[1])} for r in res.result_rows]
  return {"data": rows}
