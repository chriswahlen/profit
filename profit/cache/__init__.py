"""
Caching and local storage utilities used by fetchers.

The default `FileCache` provides simple pickle-based persistence. `SqliteStore`
offers a lightweight, queryable store that feels closer to Parquet while
remaining dependency-light.
"""

from .columnar_store import (
    ColumnarSqliteStore,
    ColumnarStoreError,
    SeriesNotFoundError,
    SliceCorruptionError,
)
from .file_cache import CacheMissError, FileCache, OfflineModeError
from .sqlite_store import DatasetNotFoundError, SchemaError, SqliteStore

__all__ = [
    "CacheMissError",
    "ColumnarSqliteStore",
    "ColumnarStoreError",
    "SeriesNotFoundError",
    "SliceCorruptionError",
    "FileCache",
    "OfflineModeError",
    "DatasetNotFoundError",
    "SchemaError",
    "SqliteStore",
]
