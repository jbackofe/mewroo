from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf

from .common import ch, utc_now_s, get_state, set_state


def _get_latest_membership_tickers(client) -> list[str]:
    q = """
    SELECT DISTINCT ticker
    FROM finance_yf.industry_membership
    WHERE asof_date = (SELECT max(asof_date) FROM finance_yf.industry_membership)
    """
    return [r[0] for r in client.query(q).result_rows]


def normalize_download(df: pd.DataFrame, tickers: list[str], interval: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    multi = isinstance(df.columns, pd.MultiIndex)
    parts = []

    for t in tickers:
        if multi:
            if t not in df.columns.get_level_values(0):
                continue
            sub = df[t].copy()
        else:
            sub = df.copy()

        if sub.empty:
            continue

        sub = sub.reset_index()
        if "Adj Close" in sub.columns:
            sub.rename(columns={"Adj Close": "Adj_Close"}, inplace=True)

        ts_col = "Date" if "Date" in sub.columns else sub.columns[0]
        sub.rename(columns={ts_col: "ts"}, inplace=True)
        sub["ts"] = pd.to_datetime(sub["ts"], utc=True, errors="coerce").dt.floor("s")

        # Ensure expected columns
        for col in ["Open", "High", "Low", "Close", "Adj_Close", "Volume"]:
            if col not in sub.columns:
                sub[col] = 0.0 if col != "Volume" else 0

        sub["ticker"] = t
        sub["interval"] = interval
        sub = sub.dropna(subset=["ts"])

        parts.append(sub[["ts", "ticker", "interval", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]])

        if not multi:
            break

    if not parts:
        return pd.DataFrame()

    out = pd.concat(parts, ignore_index=True)
    out.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj_Close": "adj_close",
            "Volume": "volume",
        },
        inplace=True,
    )

    # sanitize numerics
    for c in ["open", "high", "low", "close", "adj_close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").fillna(0).astype("int64")

    # drop rows with no usable prices
    out = out.dropna(subset=["close"])
    return out


def ingest_stock_prices(
    tickers: list[str] | None = None,
    interval: str = "1d",
    overlap_days: int = 5,
    default_lookback_days: int = 370,
    chunk_size: int = 50,
    force: bool = False,
) -> int:
    client = ch()
    ingested_at = utc_now_s()

    if not tickers:
        tickers = _get_latest_membership_tickers(client)

    # normalize tickers input
    tickers = [str(t).strip() for t in tickers if str(t).strip() and str(t).strip() != "nan"]

    def chunked(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    total = 0

    for chunk in chunked(tickers, chunk_size):
        # compute earliest start across chunk
        earliest = ingested_at - timedelta(days=default_lookback_days)

        if not force:
            for t in chunk:
                last_ts, _ = get_state(client, "yfinance", "fact_stock_prices", f"{t}|{interval}")
                if last_ts:
                    if last_ts.tzinfo is None:
                        last_ts = last_ts.replace(tzinfo=timezone.utc)
                    start = last_ts - timedelta(days=overlap_days)
                    if start < earliest:
                        earliest = start

        raw = yf.download(
            tickers=" ".join(chunk),
            start=earliest.date().isoformat(),
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            actions=True,
            threads=True,
            progress=False,
            timeout=30,
        )

        norm = normalize_download(raw, chunk, interval)
        if norm.empty:
            continue

        # insert per ticker using state to filter > last_ts
        for t in chunk:
            sub = norm[norm["ticker"] == t].copy()
            if sub.empty:
                continue

            if not force:
                last_ts, _ = get_state(client, "yfinance", "fact_stock_prices", f"{t}|{interval}")
                if last_ts:
                    if last_ts.tzinfo is None:
                        last_ts = last_ts.replace(tzinfo=timezone.utc)
                    sub = sub[sub["ts"] > last_ts]

            if sub.empty:
                continue

            records = []
            for r in sub.itertuples(index=False):
                ts = r.ts.to_pydatetime()
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                else:
                    ts = ts.astimezone(timezone.utc)

                # adj_close may be missing -> fall back to close
                adj = float(r.adj_close) if pd.notna(r.adj_close) else float(r.close)

                records.append(
                    (
                        ts,
                        t,
                        interval,
                        float(r.open) if pd.notna(r.open) else float(r.close),
                        float(r.high) if pd.notna(r.high) else float(r.close),
                        float(r.low) if pd.notna(r.low) else float(r.close),
                        float(r.close),
                        adj,
                        int(r.volume) if r.volume is not None else 0,
                        "yfinance",
                        ingested_at,
                    )
                )

            client.insert(
                "finance_yf.fact_stock_prices",
                records,
                column_names=[
                    "ts",
                    "ticker",
                    "interval",
                    "open",
                    "high",
                    "low",
                    "close",
                    "adj_close",
                    "volume",
                    "source",
                    "ingested_at",
                ],
            )

            max_ts = max(rec[0] for rec in records)
            set_state(client, "yfinance", "fact_stock_prices", f"{t}|{interval}", last_ts=max_ts)
            total += len(records)

    return total


def main():
    p = argparse.ArgumentParser(description="Ingest yfinance stock prices into ClickHouse")
    p.add_argument("--tickers", nargs="*", default=None, help="Tickers to ingest. If omitted, uses latest industry_membership.")
    p.add_argument("--interval", default="1d", help="yfinance interval (default: 1d)")
    p.add_argument("--overlap-days", type=int, default=5, help="Overlap days for incremental safety (default: 5)")
    p.add_argument("--lookback-days", type=int, default=370, help="Default lookback if no state (default: 370)")
    p.add_argument("--chunk-size", type=int, default=50, help="Tickers per yfinance.download call (default: 50)")
    p.add_argument("--force", action="store_true", help="Force re-fetch (still idempotent due to ReplacingMergeTree)")
    args = p.parse_args()

    n = ingest_stock_prices(
        tickers=args.tickers,
        interval=args.interval,
        overlap_days=args.overlap_days,
        default_lookback_days=args.lookback_days,
        chunk_size=args.chunk_size,
        force=args.force,
    )
    print(f"[fact_stock_prices] inserted_rows={n}")


if __name__ == "__main__":
    main()
