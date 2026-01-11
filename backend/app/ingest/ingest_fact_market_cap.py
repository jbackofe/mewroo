"""
Ingest market cap snapshots from yfinance into finance_yf.fact_market_cap.

Idempotency:
- One snapshot row per (ticker, asof_date). Re-running is safe due to
  ReplacingMergeTree(ingested_at) ORDER BY (ticker, asof_date).

State:
- finance_yf.ingest_state row per ticker:
  source='yfinance', target='fact_market_cap', key='<TICKER>'
  last_asof_date = asof_date
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import yfinance as yf

from .common import ch, utc_now_s, get_state, set_state


def _parse_asof_date(s: str | None) -> datetime:
    now = utc_now_s()
    if not s:
        return now.replace(hour=0, minute=0, second=0)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def _get_latest_membership_tickers(client) -> list[str]:
    q = """
    SELECT DISTINCT ticker
    FROM finance_yf.industry_membership
    WHERE asof_date = (SELECT max(asof_date) FROM finance_yf.industry_membership)
    """
    return [r[0] for r in client.query(q).result_rows]


def ingest_market_cap(
    tickers: list[str] | None = None,
    asof_date: datetime | None = None,
    force: bool = False,
) -> int:
    client = ch()
    ingested_at = utc_now_s()
    asof = asof_date or ingested_at.replace(hour=0, minute=0, second=0)

    if not tickers:
        tickers = _get_latest_membership_tickers(client)

    total_rows = 0
    batch = []

    for tkr in tickers:
        tkr = str(tkr).strip()
        if not tkr or tkr == "nan":
            continue

        # Skip if already ingested for this ticker/asof_date (unless force)
        if not force:
            _, last_asof = get_state(client, "yfinance", "fact_market_cap", tkr)
            if last_asof and last_asof >= asof:
                continue

        try:
            info = yf.Ticker(tkr).info or {}
            mc = info.get("marketCap", None)
            cur = info.get("currency", "") or ""
            if mc is None:
                # Some tickers don't provide marketCap; skip
                continue

            batch.append((asof, tkr, float(mc), str(cur), "yfinance_info", ingested_at))
            total_rows += 1

            # update per ticker state (even if batching inserts)
            set_state(client, "yfinance", "fact_market_cap", tkr, last_asof_date=asof)

        except Exception as e:
            print(f"[market_cap] ticker={tkr}: failed: {e}")
            continue

    if batch:
        client.insert(
            "finance_yf.fact_market_cap",
            batch,
            column_names=["asof_date", "ticker", "market_cap", "currency", "source", "ingested_at"],
        )

    return total_rows


def main():
    p = argparse.ArgumentParser(description="Ingest yfinance market cap snapshots")
    p.add_argument(
        "--tickers",
        nargs="*",
        default=None,
        help="Tickers to snapshot. If omitted, uses latest industry_membership tickers.",
    )
    p.add_argument(
        "--asof-date",
        default=None,
        help="Snapshot as-of date. 'YYYY-MM-DD' or ISO timestamp. Default: UTC start-of-day.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Force snapshot even if ingest_state says already done for asof-date.",
    )
    args = p.parse_args()
    asof = _parse_asof_date(args.asof_date)
    n = ingest_market_cap(tickers=args.tickers, asof_date=asof, force=args.force)
    print(f"[market_cap] inserted_rows={n} asof_date={asof.isoformat()}")


if __name__ == "__main__":
    main()
