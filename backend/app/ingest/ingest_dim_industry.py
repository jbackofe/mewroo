"""
Ingest sector -> industries mapping from yfinance into finance_yf.dim_industry.

Idempotency:
- Writes a snapshot for a given asof_date (default: UTC start-of-day).
- Re-running for the same asof_date is safe due to ReplacingMergeTree(ingested_at)
  with ORDER BY (sector_key, industry_key, asof_date).

State:
- finance_yf.ingest_state row: source='yfinance', target='dim_industry', key='ALL'
  last_asof_date = asof_date
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import yfinance as yf

from .common import ch, utc_now_s, get_state, set_state

DEFAULT_SECTORS = [
    "basic-materials",
    "communication-services",
    "consumer-cyclical",
    "consumer-defensive",
    "energy",
    "financial-services",
    "healthcare",
    "industrials",
    "real-estate",
    "technology",
    "utilities",
]


def _parse_asof_date(s: str | None) -> datetime:
    now = utc_now_s()
    if not s:
        # default to UTC start-of-day
        return now.replace(hour=0, minute=0, second=0)
    # accept 'YYYY-MM-DD' or full ISO timestamps
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def ingest_dim_industry(
    sectors: list[str] | None = None,
    asof_date: datetime | None = None,
    force: bool = False,
) -> int:
    client = ch()
    ingested_at = utc_now_s()
    asof = asof_date or ingested_at.replace(hour=0, minute=0, second=0)
    sectors = sectors or DEFAULT_SECTORS

    # Skip if already ingested for this asof_date (unless force)
    if not force:
        _, last_asof = get_state(client, "yfinance", "dim_industry", "ALL")
        if last_asof and last_asof >= asof:
            return 0

    rows = []
    for sector_key in sectors:
        try:
            sec = yf.Sector(sector_key)
            df = sec.industries
            if df is None or df.empty:
                print(f"[dim_industry] sector={sector_key}: no industries returned")
                continue

            # expected columns: key (index), name, symbol, market weight
            df = df.reset_index()
            # normalize column names
            col_map = {c: c.lower().strip() for c in df.columns}
            # ensure industry_key
            if "key" in df.columns:
                industry_key_col = "key"
            else:
                # if yfinance changes naming, try fallback
                industry_key_col = df.columns[0]

            # locate optional columns robustly
            name_col = None
            symbol_col = None
            weight_col = None
            for c in df.columns:
                lc = c.lower().strip()
                if lc == "name":
                    name_col = c
                elif lc == "symbol":
                    symbol_col = c
                elif lc in ("market weight", "market_weight", "weight"):
                    weight_col = c

            if name_col is None or symbol_col is None or weight_col is None:
                print(
                    f"[dim_industry] sector={sector_key}: unexpected columns={list(df.columns)}"
                )
                continue

            for _, r in df.iterrows():
                industry_key = str(r[industry_key_col]).strip()
                industry_name = str(r[name_col]).strip()
                symbol = str(r[symbol_col]).strip()
                try:
                    market_weight = float(r[weight_col])
                except Exception:
                    market_weight = 0.0

                if not industry_key or industry_key == "nan":
                    continue

                rows.append(
                    (
                        sector_key,
                        industry_key,
                        industry_name,
                        symbol,
                        market_weight,
                        asof,
                        ingested_at,
                    )
                )

        except Exception as e:
            print(f"[dim_industry] sector={sector_key}: failed: {e}")

    if rows:
        client.insert(
            "finance_yf.dim_industry",
            rows,
            column_names=[
                "sector_key",
                "industry_key",
                "industry_name",
                "industry_symbol",
                "market_weight",
                "asof_date",
                "ingested_at",
            ],
        )

    set_state(client, "yfinance", "dim_industry", "ALL", last_asof_date=asof)
    return len(rows)


def main():
    p = argparse.ArgumentParser(description="Ingest yfinance sector->industry mapping")
    p.add_argument(
        "--sectors",
        nargs="*",
        default=None,
        help="Sector keys (default: all major sectors). Example: technology utilities",
    )
    p.add_argument(
        "--asof-date",
        default=None,
        help="Snapshot as-of date. 'YYYY-MM-DD' or ISO timestamp. Default: UTC start-of-day.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Force ingestion even if ingest_state says already done for asof-date.",
    )
    args = p.parse_args()
    asof = _parse_asof_date(args.asof_date)
    n = ingest_dim_industry(sectors=args.sectors, asof_date=asof, force=args.force)
    print(f"[dim_industry] inserted_rows={n} asof_date={asof.isoformat()}")


if __name__ == "__main__":
    main()
