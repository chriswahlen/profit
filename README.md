## CLI helper

`scripts/fetch_equities.py` is a lightweight script that downloads daily equity bars via `yfinance`, writes raw/adjusted fields into `ColumnarSqliteStore`, and demonstrates reading back a selected field. Run it with:

```
python scripts/fetch_equities.py --ticker AAPL --start 2025-12-01 --end 2025-12-10
```

Supply `--mic` to customize the instrument identifier (defaults to `XNAS`), `--store-path` to target a different columnar cache, and `--read-fields close_raw close_adj` to inspect additional columns after ingestion.

By default the columnar database is stored under the **data root** (`PROFIT_DATA_ROOT` from `~/.profit.conf`, falling back to `./data/columnar.sqlite3`). Cache files remain under `PROFIT_CACHE_ROOT`.

`scripts/fetch_fx.py` fetches daily FX rates via `yfinance`, writes them into the same `ColumnarSqliteStore` (dataset `fx_rate:{source}:{version}`, field `rate`), and can read them back for inspection. Example:
```
python scripts/fetch_fx.py --base EUR --quote USD --start 2025-01-01 --end 2025-01-15 --read-back
```

`scripts/fetch_commodities.py` (to be added) will fetch daily commodity prices (initially gold/silver via Alpha Vantage) into `ColumnarSqliteStore` under dataset `commodity_price:{source}:{version}`.
