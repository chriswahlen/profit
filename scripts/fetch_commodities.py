from __future__ import annotations

from argparse import ArgumentParser
import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
from typing import Sequence

from profit.cache import ColumnarSqliteStore, FileCache
from profit.config import ensure_profit_conf_loaded, get_cache_root, get_columnar_db_path, add_common_cli_args
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
    parser.add_argument(
        "--refresh-catalog",
        action="store_true",
        help="Force catalog refresh before fetching.",
    )
    add_common_cli_args(
        parser,
        cache_help_subdir="commodities_fetcher",
        default_store_filename="columnar.sqlite3",
        include_catalog_path=False,
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

    store_path = get_columnar_db_path(args=args)
    store_path.parent.mkdir(parents=True, exist_ok=True)
    store = ColumnarSqliteStore(db_path=store_path)
    cfg = ColumnarCommodityConfig()
    dataset = cfg.dataset_name(source=provider, version="v1")

    from profit.sources.commodities.goldapi import GoldApiCommoditiesFetcher

    fetcher = GoldApiCommoditiesFetcher(
        cache=cache,
        api_key=args.api_key,
        max_window_days=None,
        catalog_path=store_path,
        allow_network=True,
    )

    if args.refresh_catalog:
        from profit.catalog.refresher import CatalogChecker
        from profit.sources.commodities.goldapi_refresher import GoldApiRefresher

        checker = CatalogChecker(
            store=fetcher.lifecycle.store,  # type: ignore[attr-defined]
            refresher=GoldApiRefresher(fetcher.lifecycle.store),  # type: ignore[attr-defined]
            max_age=timedelta(days=0),
            allow_network=True,
        )
        checker.refresher.refresh("goldapi", allow_network=True)
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

    print(
        f"Ensuring coverage for {request.instrument_id} {start.date()} → {end.date()} via {provider}..."
    )
    fetcher.timeseries_fetch_many([request], start, end)

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
