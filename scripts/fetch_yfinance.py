from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from profit.cache import FileCache
from profit.config import ProfitConfig, add_common_cli_args
from profit.sources.yfinance_ingest import fetch_and_store_yfinance, _parse_date
from profit.stores import StoreContainer


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch yfinance OHLCV bars into the columnar store")
    parser.add_argument("instrument_ids", help="Comma-separated canonical instrument_ids (e.g., XNAS|AAPL,XNYS|MSFT)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or ISO datetime, UTC assumed if naive)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or ISO datetime, UTC assumed if naive)")
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help="Ignore --start/--end and fetch from the last recorded day (any provider) to now",
    )
    parser.add_argument("--ttl-days", type=int, default=1, help="Cache TTL days (default 1)")
    parser.add_argument("--offline", action="store_true", help="Use cache only; skip network requests")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and log counts without writing to the columnar store")
    add_common_cli_args(parser, cache_help_subdir="yfinance")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    cfg = ProfitConfig.from_args(args)
    ProfitConfig.apply_runtime_env(cfg)

    cache = FileCache(base_dir=cfg.cache_root / "yfinance_fetcher", ttl=timedelta(days=args.ttl_days))
    stores = StoreContainer.open(cfg.store_path)

    try:
        instrument_ids = [t.strip() for t in args.instrument_ids.split(",") if t.strip()]

        if not args.catch_up:
            if not args.start or not args.end:
                parser.error("--start and --end are required unless --catch-up is used")
            start = _parse_date(args.start)
            end = _parse_date(args.end)
        else:
            # Catch-up ignores provided bounds; default to wide window if omitted.
            start = _parse_date(args.start) if args.start else datetime(1900, 1, 1, tzinfo=timezone.utc)
            end = _parse_date(args.end) if args.end else datetime.now(timezone.utc)

        fetch_and_store_yfinance(
            instrument_ids=instrument_ids,
            start=start,
            end=end,
            cfg=cfg,
            stores=stores,
            cache=cache,
            offline=args.offline,
            ttl=timedelta(days=args.ttl_days),
            dry_run=args.dry_run,
            catch_up=args.catch_up,
        )
    finally:
        stores.close()


if __name__ == "__main__":
    main()
