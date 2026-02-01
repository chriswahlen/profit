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
    parser.add_argument(
        "instrument_ids",
        nargs="?",
        default="",
        help="Comma-separated canonical instrument_ids (e.g., XNAS|AAPL,XNYS|MSFT). "
        "When omitted with --catch-up, all yfinance instruments in the catalog are processed.",
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD or ISO datetime, UTC assumed if naive)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD or ISO datetime, UTC assumed if naive)")
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help="Ignore --start/--end and fetch from the last recorded day (any provider) to now",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Fetch all yfinance instruments known in the catalog with catch-up windows (implies --catch-up)",
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
    store_path = Path(cfg.store_path).resolve()
    stores = StoreContainer.open(store_path)

    def _load_all_instruments(require_existing_data: bool = False, prefix: str | None = None):
        if not require_existing_data:
            query = "SELECT DISTINCT instrument_id FROM instrument_provider_map"
            params: list[str] = []
            if prefix:
                query += " WHERE instrument_id LIKE ?"
                params.append(f"{prefix}%")
            query += " ORDER BY instrument_id"
            cur = stores.catalog.conn.execute(query, params)
            return [row[0] for row in cur.fetchall()]

        query = "SELECT DISTINCT instrument_id FROM __col_series__"
        params = []
        if prefix:
            query += " WHERE instrument_id LIKE ?"
            params.append(f"{prefix}%")
        query += " ORDER BY instrument_id"
        cur = stores.catalog.conn.execute(query, params)
        return [row[0] for row in cur.fetchall()]

    try:
        if args.backfill:
            args.catch_up = True
        instrument_ids = [t.strip() for t in args.instrument_ids.split(",") if t.strip()]
        if not instrument_ids and args.backfill:
            logging.info("backfill mode selecting instruments with existing data prefix=XNAS|*")
            instrument_ids = _load_all_instruments(require_existing_data=True, prefix="XNAS|")
        elif not instrument_ids and args.catch_up:
            instrument_ids = _load_all_instruments(prefix="XNAS|")
            logging.info("catch-up all mode: found %s instruments", len(instrument_ids))
        if not instrument_ids:
            parser.error("instrument_ids required unless --catch-up with catalog-backed all-instruments is used")

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
