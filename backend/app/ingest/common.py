import os
from datetime import datetime, timezone
import clickhouse_connect

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_DB = os.getenv("CLICKHOUSE_DATABASE", "default")
CH_USER = os.getenv("CLICKHOUSE_USER", "mewroo")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "mewroo")

def ch():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, database=CH_DB, username=CH_USER, password=CH_PASSWORD
    )

def utc_now_s():
    return datetime.now(timezone.utc).replace(microsecond=0)

def get_state(client, source: str, target: str, key: str):
    q = """
    SELECT last_ts, last_asof_date
    FROM finance_yf.ingest_state
    WHERE source=%(source)s AND target=%(target)s AND key=%(key)s
    ORDER BY updated_at DESC
    LIMIT 1
    """
    r = client.query(q, parameters={"source": source, "target": target, "key": key}).result_rows
    return r[0] if r else (None, None)

def set_state(client, source: str, target: str, key: str, last_ts=None, last_asof_date=None):
    client.insert(
        "finance_yf.ingest_state",
        [(source, target, key, last_ts, last_asof_date)],
        column_names=["source", "target", "key", "last_ts", "last_asof_date"],
    )
