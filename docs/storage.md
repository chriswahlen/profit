# Local Storage Guide

This repo ships three lightweight storage layers:
- **FileCache**: pickle files, opaque payloads, TTL-based.
- **SqliteStore**: queryable tables that feel closer to Parquet while staying dependency-light.
- **ColumnarSqliteStore**: fixed-step numeric time series stored as packed slices (BLOBs) with per-slice overwrite.

This page documents both SqliteStore (row/table-oriented) and ColumnarSqliteStore (column-slice-oriented).

## Philosophy
- Deterministic, provider-neutral storage with explicit schemas.
- Append-first. Overwrites are only allowed when you opt in and define primary keys.
- UTC-first: timestamps are stored as ISO8601 with `Z`.
- No hidden state: schema lives in the same SQLite file in `__dataset_schema__`.

## File layout
- Default path: `.cache/profit/cache.sqlite3` (or `$PROFIT_CACHE_DIR/cache.sqlite3`).
- Each dataset is a SQLite table; schema metadata is mirrored in `__dataset_schema__`.
- Dependencies: stdlib `sqlite3` only. Pandas is optional for DataFrame reads.

## ColumnarSqliteStore (Packed Slices)

ColumnarSqliteStore is a separate SQLite-backed storage mode intended for fixed-step numeric time series (v1: `float64` only). It stores one column (field) per series and stores values in packed arrays inside BLOBs.

Key ideas:
- A **series** defines the grid and decoding rules (step, origin, window size, compression, sentinel).
- A **slice** is one canonical window for that series (fixed `window_points` length).
- Writes are provided as **timestamp/value pairs**; the store reads the impacted slices, overlays updates, and rewrites canonical slices with `INSERT OR REPLACE`.
- There are no NULLs inside arrays; missing points are represented by a **sentinel** (recommended: NaN).

### Storage layout
- Default path: `.cache/profit/columnar.sqlite3` (or `$PROFIT_CACHE_DIR/columnar.sqlite3`).
- Series metadata table: `__col_series__`
- Slice table: `__col_slice__` with primary key `(series_id, start_index)`

`start_index` is a grid index relative to the series origin:
- `index = floor((ts_us - grid_origin_ts_us) / step_us)`
- `start_index` is aligned: `start_index % window_points == 0`

### Creating a series
```python
from profit.cache import ColumnarSqliteStore

store = ColumnarSqliteStore()
series_id = store.create_series(
    instrument_id="AAPL",
    dataset="bar_ohlcv",
    field="close",
    step_us=86_400_000_000,  # daily UTC grid
    grid_origin_ts_us=0,     # unix epoch origin
    window_points=365,       # one-year slices
    compression="zlib",      # per-series toggle
    offsets_enabled=False,   # optional time offsets per point
    checksum_enabled=True,   # verify slice payload integrity
    sentinel_f64=float("nan"),
)
```

Notes:
- `sentinel_f64` is stored as float64 bits (INTEGER) because Python sqlite bindings treat NaN as NULL for REAL bindings.
- v1 is numeric-only and `float64` only. Strings/JSON are out of scope for this store.

### Writing points (timestamp/value pairs)
```python
from datetime import datetime, timezone

store.write(
    series_id,
    [
        (datetime(2020, 1, 1, tzinfo=timezone.utc), 100.0),
        (datetime(2020, 1, 2, tzinfo=timezone.utc), float("nan")),  # explicit sentinel overwrites
    ],
)
```

Semantics:
- If a provided timestamp is **before** `grid_origin_ts_us`, `write()` raises `ValueError`.
- If a provided value equals the sentinel, it **overwrites** any existing value (i.e., you can explicitly clear a point).

### Optional offsets
If `offsets_enabled=True`, the store also records an `int32` millisecond offset per point inside each slice. This allows preserving an “actual event time” distinct from the nominal fixed-step grid timestamp while still indexing on the grid.

## Creating a dataset
```python
from profit.cache import SqliteStore
store = SqliteStore()  # or SqliteStore(Path("/tmp/custom.sqlite3"))

schema = {
    "instrument_id": "TEXT",
    "ts_utc": "TIMESTAMP",
    "close": "REAL",
    "volume": "INTEGER",
}
store.create_dataset(
    "bar_ohlcv",
    schema,
    primary_keys=["instrument_id", "ts_utc"],  # required for overwrite/upserts
)
```

### Types
Supported column types: `TEXT`, `INTEGER`, `REAL`, `BLOB`, `TIMESTAMP`.
- `TIMESTAMP` is stored as UTC ISO8601 (`2020-01-01T00:00:00Z`) and always decoded to an aware `datetime` in UTC.
- `bool` values map to `INTEGER` (0/1) when inferred.

## Appending data
```python
rows = [
    {"instrument_id": "AAPL", "ts_utc": ts1, "close": 150.1, "volume": 1_000},
    {"instrument_id": "AAPL", "ts_utc": ts2, "close": 151.2, "volume": 900},
]
store.append("bar_ohlcv", rows)
```

- `create=True` lets you infer schema on first append:
  ```python
  store.append("fx_rate", rows, create=True)
  ```
- `overwrite=True` performs `INSERT OR REPLACE` using the declared primary key(s).

## Reading data
Column projection, predicate pushdown, ordering, and limits are supported.
```python
msft = store.read(
    "bar_ohlcv",
    columns=["ts_utc", "close"],
    where="instrument_id = :code AND ts_utc >= :start",
    params={"code": "MSFT", "start": "2020-05-01T00:00:00Z"},
    order_by=["ts_utc"],
    as_dataframe=False,  # default: auto DataFrame if pandas installed
)
```

## Managing datasets
- `store.get_schema(name)` → list of `ColumnDef`.
- `store.drop_dataset(name)` drops the table and its schema metadata.
- Missing datasets raise `DatasetNotFoundError`; bad types/columns raise `SchemaError`.

## When to choose ColumnarSqliteStore vs SqliteStore vs FileCache
- Use **ColumnarSqliteStore** when you want:
  - Fixed-step `float64` time series with predictable canonical windows.
  - Packed arrays (BLOBs) and chunk pruning by canonical slice window.
  - Timestamp/value pair writes that rewrite canonical slices atomically.
- Use **SqliteStore** when you want:
  - Structured, queryable local data (projection/predicate) without pulling entire payloads.
  - PK-aware overwrites for idempotent ingestion runs.
  - Optional DataFrame interop without requiring Parquet/DuckDB dependencies.
- Use **FileCache** when you want:
  - Arbitrary Python objects (pickled) with minimal ceremony.
  - Strict FIFO cache semantics with TTL and no schema management.

## Testing expectations
- See `tests/test_sqlite_store.py` for round-trip examples, projections, overwrite behavior, and schema inference coverage. Tests run offline; no external I/O.
- See `tests/test_columnar_store.py` for canonical slice rewrite behavior, sentinel overwrites, offsets, and error handling.
