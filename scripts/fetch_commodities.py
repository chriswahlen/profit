from __future__ import annotations

from argparse import ArgumentParser
import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
from typing import Sequence

from profit.cache import ColumnarSqliteStore, FileCache
from profit.catalog import CatalogStore
from profit.catalog.lifecycle import CatalogLifecycleReader
from profit.config import ensure_profit_conf_loaded, get_cache_root, get_columnar_db_path, add_common_cli_args, get_catalog_db_path
from profit.sources.commodities.base import CommodityDailyRequest
from profit.sources.commodities.columnar import ColumnarCommodityConfig, DAY_US


DATE_FMT = "%Y-%m-%d"
DATE_FMT_HELP = DATE_FMT.replace("%", "%%")


INSTRUMENT_MAP = {
    "gold": {"instrument_id": "XAU|LBMA", "provider_code": "GOLD", "metal_code": "XAU"},
    "silver": {"instrument_id": "XAG|LBMA", "provider_code": "SILVER", "metal_code": "XAG"},
}


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, DATE_FMT).replace(tzinfo=timezone.utc)


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Fetch daily commodity prices.")
    parser.add_argument("--commodity", choices=sorted(INSTRUMENT_MAP.keys()), required=False, help="Commodity to fetch")
    parser.add_argument(
        "--start",
        required=False,
        help=f"Inclusive start date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--end",
        required=False,
        help=f"Inclusive end date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Provider API key (Alpha Vantage: ALPHAVANTAGE_API_KEY, GoldAPI: GOLDAPI_API_KEY)",
    )
    parser.add_argument(
        "--provider",
        choices=["goldapi"],
        default="goldapi",
        help="Commodity data provider (only goldapi supported currently)",
    )
    parser.add_argument(
        "--read-back",
        action="store_true",
        help="Read back inserted rows and print them.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Skip using on-disk cache (forces network fetch; uses in-memory cache only).",
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Print fetcher capabilities and exit.",
    )
    add_common_cli_args(
        parser,
        cache_help_subdir="commodities_fetcher",
        default_store_filename="columnar.sqlite3",
        include_catalog_path=True,
        default_catalog_filename="catalog.sqlite3",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    ensure_profit_conf_loaded()
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if args.no_cache:
        cache = FileCache(base_dir=None, ttl=timedelta(seconds=0))
    else:
        base_cache_dir = Path(get_cache_root(args=args))
        base_cache_dir.mkdir(parents=True, exist_ok=True)
        cache = FileCache(base_dir=base_cache_dir / "commodities_fetcher")

    store = ColumnarSqliteStore(db_path=get_columnar_db_path(args=args))
    cfg = ColumnarCommodityConfig()
    dataset = cfg.dataset_name(source=provider, version="v1")

    from profit.sources.commodities.goldapi import GoldApiCommoditiesFetcher

    catalog_path = get_catalog_db_path(args=args)
    if not catalog_path.exists():
        parser.error(f"Catalog not found at {catalog_path}; lifecycle metadata required.")
    catalog_store = CatalogStore(catalog_path, readonly=True)
    lifecycle = CatalogLifecycleReader(catalog_store)

    fetcher = GoldApiCommoditiesFetcher(
        cache=cache,
        api_key=args.api_key,
        max_window_days=None,
        lifecycle=lifecycle,
    )
    # Inject store for coverage adapter
    fetcher._coverage_store = store  # type: ignore[attr-defined]

    if args.describe:
        desc = fetcher.describe()
        print("Fetcher capabilities:")
        print(f"  provider   : {desc.provider}")
        print(f"  dataset    : {desc.dataset}")
        print(f"  version    : {desc.version}")
        print(f"  freqs      : {', '.join(desc.freqs)}")
        print(f"  fields     : {', '.join(desc.fields)}")
        print(f"  max_window : {desc.max_window_days}")
        if desc.notes:
            print(f"  notes      : {desc.notes}")
        return

    for name in ("commodity", "start", "end"):
        if getattr(args, name) is None:
            parser.error(f"--{name} is required unless --describe is used")

    mapping = INSTRUMENT_MAP[args.commodity]
    provider = args.provider
    request = CommodityDailyRequest(
        instrument_id=mapping["instrument_id"],
        provider=provider,
        provider_code=mapping["metal_code"],
    )

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if start > end:
        parser.error("--start must be <= --end")

    clipped_start, clipped_end = start, end
    try:
        catalog_path = get_catalog_db_path(args=args)
        catalog_store = CatalogStore(catalog_path, readonly=True)
        li = lookup_and_clip(
            catalog_store,
            provider=provider,
            provider_code=request.provider_code,
            start=start,
            end=end,
            hard_fail=True,
        )
        if li:
            if li.is_empty:
                parser.error(
                    f"Requested window {start.date()}–{end.date()} is outside lifecycle "
                    f"{li.active_start.date()}–{(li.active_end.date() if li.active_end else 'open')}"
                )
            if not li.is_full:
                print(
                    f"Clipping to active lifecycle window {li.clipped_start.date()} → {li.clipped_end.date()} "
                    f"(requested {start.date()} → {end.date()}, active {li.active_start.date()} → "
                    f"{li.active_end.date() if li.active_end else 'open'})"
                )
            clipped_start, clipped_end = li.clipped_start, li.clipped_end
    except Exception:
        pass

    print(
        f"Ensuring coverage for {request.instrument_id} {clipped_start.date()} → {clipped_end.date()} via {provider}..."
    )
    fetcher.timeseries_fetch_many([request], clipped_start, clipped_end)

    if args.read_back:
        series_id = store.get_series_id(
            instrument_id=request.instrument_id,
            dataset=dataset,
            field="price",
            step_us=DAY_US,
        )
        if series_id is None:
            print("No series found for read-back.")
            return
        pts = store.read_points(series_id, start=start, end=end, include_sentinel=False)
        print(f"Read back {len(pts)} points:")
        for ts, price in pts:
            print(f"  {ts.date().isoformat()} price={price}")


if __name__ == "__main__":
    main()
