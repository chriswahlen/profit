from __future__ import annotations

from argparse import ArgumentParser
import logging
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

from profit.cache import ColumnarSqliteStore, FileCache
from profit.cache.file_cache import _default_cache_dir
from profit.config import ensure_profit_conf_loaded, get_data_root
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
        "--cache-dir",
        type=Path,
        default=None,
        help="Directory for fetcher cache (default: PROFIT_CACHE_* + '/fx_fetcher')",
    )
    parser.add_argument(
        "--store-path",
        type=Path,
        default=None,
        help="Path to ColumnarSqliteStore (default: PROFIT_DATA_ROOT/columnar.sqlite3)",
    )
    parser.add_argument(
        "--read-back",
        action="store_true",
        help="Read back inserted rows and print them.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING...). Default: INFO",
    )
    return parser


def main() -> None:
    ensure_profit_conf_loaded()
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    start = _parse_date(args.start)
    end = _parse_date(args.end)
    if start > end:
        parser.error("--start must be <= --end")

    provider_code = args.provider_code or f"{args.base}{args.quote}=X"

    base_cache_dir = Path(
        args.cache_dir
        or os.environ.get("PROFIT_CACHE_DIR")
        or os.environ.get("PROFIT_CACHE_ROOT")
        or _default_cache_dir()
    )
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

    store_path = args.store_path or get_data_root() / "columnar.sqlite3"
    store = ColumnarSqliteStore(store_path)
    cfg = ColumnarFxConfig()
    dataset = cfg.dataset_name(source="yfinance", version="v1")
    pair = f"{req.base_ccy}/{req.quote_ccy}"
    series_id = store.get_series_id(
        instrument_id=pair,
        dataset=dataset,
        field="rate",
        step_us=cfg.step_us,
    )
    if series_id is not None and store.is_range_complete(series_id, start=start, end=end):
        print("Range already complete in columnar store; skipping fetch.")
        pts = store.read_points(series_id, start=start, end=end, include_sentinel=False)
        print(f"Read back {len(pts)} points:")
        for ts, rate in pts:
            print(f"  {ts.date().isoformat()} rate={rate}")
        return

    cache = FileCache(base_dir=base_cache_dir / "fx_fetcher")
    fetcher = YFinanceFxDailyFetcher(cache=cache, max_window_days=30)
    print(f"Fetching FX {req.base_ccy}/{req.quote_ccy} {start.date()} → {end.date()} via yfinance...")
    rates = fetcher.timeseries_fetch(req, start, end)
    if not rates:
        print("Provider returned no rates.")
        return

    writer = ColumnarFxWriter(store, cfg=ColumnarFxConfig())
    inserted = writer.write_rates(rates, coverage_start=start, coverage_end=end)
    print(f"Wrote {inserted} FX points to {store_path}")

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
