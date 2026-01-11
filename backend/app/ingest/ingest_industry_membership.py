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


def ingest_membership(asof_date=None, force: bool = False) -> int:
    client = ch()
    ingested_at = utc_now_s()
    asof = asof_date or ingested_at.replace(hour=0, minute=0, second=0)

    # skip if already ingested for this asof
    if not force:
        _, last_asof = get_state(client, "yfinance", "industry_membership", "ALL")
        if last_asof and last_asof >= asof:
            return 0

    dim = client.query(
        """
        SELECT
          sector_key,
          industry_key,
          argMax(industry_name, asof_date) AS industry_name
        FROM finance_yf.dim_industry
        GROUP BY sector_key, industry_key
        """
    ).result_rows

    rows = []
    for sector_key, industry_key, industry_name in dim:
        try:
            ind = yf.Industry(industry_key)
            top = ind.top_companies
            if top is None or top.empty:
                continue

            cols = [c.lower() for c in top.columns]

            # robust ticker column detection
            sym_col = None
            for cand in ["symbol", "ticker"]:
                if cand in cols:
                    sym_col = top.columns[cols.index(cand)]
                    break
            if sym_col is None:
                # Sometimes the index contains tickers
                if top.index.name and "symbol" in str(top.index.name).lower():
                    # If index is symbol-like, take it
                    top = top.reset_index()
                    cols = [c.lower() for c in top.columns]
                    for cand in ["symbol", "ticker"]:
                        if cand in cols:
                            sym_col = top.columns[cols.index(cand)]
                            break
            if sym_col is None:
                continue

            # name column detection
            name_col = None
            for cand in ["name", "longname", "shortname"]:
                if cand in cols:
                    name_col = top.columns[cols.index(cand)]
                    break

            for _, r in top.iterrows():
                ticker = str(r[sym_col]).strip()
                if not ticker or ticker == "nan":
                    continue

                tname = str(r[name_col]).strip() if name_col else ""
                rows.append((asof, sector_key, industry_key, ticker, tname, "yfinance_top_companies", ingested_at))

        except Exception as e:
            # yfinance may 404 for some industry keys; skip
            print(f"[membership] industry_key={industry_key}: failed: {e}")
            continue

    if rows:
        client.insert(
            "finance_yf.industry_membership",
            rows,
            column_names=["asof_date", "sector_key", "industry_key", "ticker", "ticker_name", "source", "ingested_at"],
        )

    set_state(client, "yfinance", "industry_membership", "ALL", last_asof_date=asof)
    return len(rows)


def main():
    p = argparse.ArgumentParser(description="Ingest yfinance industry membership into ClickHouse")
    p.add_argument("--asof-date", default=None, help="As-of date 'YYYY-MM-DD' or ISO timestamp (default: UTC start-of-day)")
    p.add_argument("--force", action="store_true", help="Force ingestion even if already done for asof-date")
    args = p.parse_args()

    asof = _parse_asof_date(args.asof_date)
    n = ingest_membership(asof_date=asof, force=args.force)
    print(f"[industry_membership] inserted_rows={n} asof_date={asof.isoformat()}")


if __name__ == "__main__":
    main()
