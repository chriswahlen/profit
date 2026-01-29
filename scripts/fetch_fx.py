from __future__ import annotations

from argparse import ArgumentParser
import logging
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

from profit.cache import ColumnarSqliteStore, FileCache
from profit.config import ensure_profit_conf_loaded, get_cache_root, get_columnar_db_path, add_common_cli_args
from profit.sources.fx import (
    ColumnarFxConfig,
    ColumnarFxWriter,
    DAY_US,
    FxRequest,
    YFinanceFxDailyFetcher,
)


DATE_FMT = "%Y-%m-%d"
DATE_FMT_HELP = DATE_FMT.replace("%", "%%")


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, DATE_FMT).replace(tzinfo=timezone.utc)


def _build_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Fetch daily FX rates and store them in ColumnarSqliteStore.")
    parser.add_argument("--base", required=True, help="Base currency (e.g., EUR)")
    parser.add_argument("--quote", required=True, help="Quote currency (e.g., USD)")
    parser.add_argument(
        "--provider-code",
        default=None,
        help="Provider symbol (default: BASEQUOTE=X for yfinance)",
    )
    parser.add_argument(
        "--start",
        required=True,
        help=f"Inclusive start date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--end",
        required=True,
        help=f"Inclusive end date in {DATE_FMT_HELP} format (UTC)",
    )
    parser.add_argument(
        "--read-back",
        action="store_true",
        help="Read back inserted rows and print them.",
    )
    add_common_cli_args(parser, cache_help_subdir="fx_fetcher", default_store_filename="columnar.sqlite3")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    ensure_profit_conf_loaded()
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if start > end:
        parser.error("--start must be <= --end")

    provider_code = args.provider_code or f"{args.base}{args.quote}=X"

    base_cache_dir = Path(get_cache_root(args=args))
    base_cache_dir.mkdir(parents=True, exist_ok=True)
    yf_cache_dir = base_cache_dir / "yfinance"
    os.environ.setdefault("YFINANCE_CACHE_DIR", str(yf_cache_dir))
    yf_cache_dir.mkdir(parents=True, exist_ok=True)

    req = FxRequest(
        base_ccy=args.base.upper(),
        quote_ccy=args.quote.upper(),
        provider="yfinance",
        provider_code=provider_code,
    )

    store = ColumnarSqliteStore(get_columnar_db_path(args=args))
    cfg = ColumnarFxConfig()
    dataset = cfg.dataset_name(source="yfinance", version="v1")
    pair = f"{req.base_ccy}/{req.quote_ccy}"
    cache = FileCache(base_dir=base_cache_dir / "fx_fetcher")
    fetcher = YFinanceFxDailyFetcher(cache=cache, store=store)

    print(f"Ensuring coverage for FX {req.base_ccy}/{req.quote_ccy} {start.date()} → {end.date()} via yfinance...")
    fetcher.timeseries_fetch(req, start, end)

    if args.read_back:
        series_id = store.get_series_id(
            instrument_id=pair,
            dataset=dataset,
            field="rate",
            step_us=DAY_US,
        )
        if series_id is None:
            print("No series found for read-back.")
            return
        pts = store.read_points(series_id, start=start, end=end, include_sentinel=False)
        print(f"Read back {len(pts)} points:")
        for ts, rate in pts:
            print(f"  {ts.date().isoformat()} rate={rate}")


if __name__ == "__main__":
    main()
