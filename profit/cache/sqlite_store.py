from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence

from .file_cache import _default_cache_dir


class SchemaError(ValueError):
    """Raised when a dataset schema is invalid or mismatched."""


class DatasetNotFoundError(KeyError):
    """Raised when attempting to use an unknown dataset."""


Identifier = str


@dataclass(frozen=True)
class ColumnDef:
    name: Identifier
    declared_type: str
    is_primary_key: bool = False


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote(identifier: Identifier) -> str:
    if not _IDENT_RE.match(identifier):
        raise SchemaError(f"Invalid identifier: {identifier!r}")
    return f'"{identifier}"'


def _default_db_path() -> Path:
    return _default_cache_dir() / "cache.sqlite3"


def _canonical_type(py_val: Any) -> str:
    if isinstance(py_val, bool):
        return "INTEGER"
    if isinstance(py_val, int):
        return "INTEGER"
    if isinstance(py_val, float):
        return "REAL"
    if isinstance(py_val, (bytes, bytearray, memoryview)):
        return "BLOB"
    if isinstance(py_val, datetime):
        return "TIMESTAMP"
    return "TEXT"


def _encode_value(value: Any, declared_type: str) -> Any:
    if value is None:
        return None

    if declared_type == "TIMESTAMP":
        if not isinstance(value, datetime):
            raise TypeError(f"Expected datetime for TIMESTAMP column; got {type(value)}")
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if declared_type == "INTEGER" and isinstance(value, bool):
        return int(value)

    return value


def _decode_value(value: Any, declared_type: str) -> Any:
    if value is None:
        return None
    if declared_type == "TIMESTAMP":
        # Stored as ISO 8601 string with trailing Z.
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    if declared_type == "INTEGER":
        # Preserve bools when possible.
        if value in (0, 1):
            return bool(value) if isinstance(value, int) else value
    return value


class SqliteStore:
    """
    Lightweight SQLite-backed dataset store with parquet-like ergonomics.

    - Column projection: select a subset of columns efficiently.
    - Predicate pushdown: supply a `WHERE` clause with parameters.
    - Append-only writes by default; `overwrite=True` allows replacing rows that
      collide on the declared primary key.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = Path(db_path) if db_path else _default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # Use SQLite's internal statement cache (default 128). Make it explicit so
        # higher-throughput callers can tune it here if needed.
        self._conn = sqlite3.connect(self.db_path, cached_statements=256)
        # Favor concurrent readers and durability suited for cache-style workloads.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.row_factory = sqlite3.Row
        self._init_metadata()

    # Public API -----------------------------------------------------
    def create_dataset(
        self,
        name: Identifier,
        schema: Mapping[Identifier, str],
        *,
        primary_keys: Sequence[Identifier] | None = None,
        if_not_exists: bool = True,
    ) -> None:
        columns = self._normalize_schema(schema, primary_keys)
        clause = "IF NOT EXISTS " if if_not_exists else ""
        column_sql = ", ".join(
            f"{_quote(col.name)} {col.declared_type}"
            + (" PRIMARY KEY" if col.is_primary_key and len(primary_keys or []) == 1 else "")
            for col in columns
        )
        pk_sql = ""
        pk_columns = [c.name for c in columns if c.is_primary_key]
        if len(pk_columns) > 1:
            pk_sql = f", PRIMARY KEY ({', '.join(_quote(c) for c in pk_columns)})"
        sql = f"CREATE TABLE {clause}{_quote(name)} ({column_sql}{pk_sql})"
        cur = self._conn.cursor()
        cur.execute(sql)
        self._persist_schema(name, columns)
        self._conn.commit()

    def append(
        self,
        name: Identifier,
        rows: Iterable[Mapping[str, Any]],
        *,
        create: bool = False,
        schema: Mapping[Identifier, str] | None = None,
        primary_keys: Sequence[Identifier] | None = None,
        overwrite: bool = False,
    ) -> int:
        rows = list(rows)
        if not rows:
            return 0

        if not self._dataset_exists(name):
            if not create:
                raise DatasetNotFoundError(name)
            inferred_schema = schema or self._infer_schema_from_rows(rows)
            self.create_dataset(name, inferred_schema, primary_keys=primary_keys)

        columns = self._load_schema(name)
        column_names = [c.name for c in columns]
        placeholders = ", ".join(f":{c}" for c in column_names)
        verb = "INSERT OR REPLACE" if overwrite and any(c.is_primary_key for c in columns) else "INSERT"
        sql = f"{verb} INTO {_quote(name)} ({', '.join(_quote(c) for c in column_names)}) VALUES ({placeholders})"

        encoded_rows = []
        for row in rows:
            encoded: MutableMapping[str, Any] = {}
            for col in columns:
                declared_type = col.declared_type
                encoded[col.name] = _encode_value(row.get(col.name), declared_type)
            encoded_rows.append(encoded)

        cur = self._conn.cursor()
        cur.executemany(sql, encoded_rows)
        self._conn.commit()
        return len(encoded_rows)

    def read(
        self,
        name: Identifier,
        *,
        columns: Sequence[Identifier] | None = None,
        where: str | None = None,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        order_by: Sequence[Identifier] | None = None,
        limit: int | None = None,
        as_dataframe: bool | None = None,
    ):
        schema = self._load_schema(name)
        col_lookup = {c.name: c for c in schema}

        if columns is None:
            select_cols = schema
        else:
            select_cols = []
            for col in columns:
                if col not in col_lookup:
                    raise SchemaError(f"Unknown column {col!r} for dataset {name!r}")
                select_cols.append(col_lookup[col])

        select_clause = ", ".join(_quote(c.name) for c in select_cols)
        sql = f"SELECT {select_clause} FROM {_quote(name)}"

        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += " ORDER BY " + ", ".join(_quote(c) for c in order_by)
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        cur = self._conn.cursor()
        cur.execute(sql, params or [])
        raw_rows = cur.fetchall()

        decoded_rows = []
        for row in raw_rows:
            as_dict = {}
            for col in select_cols:
                as_dict[col.name] = _decode_value(row[col.name], col.declared_type)
            decoded_rows.append(as_dict)

        if as_dataframe is False:
            return decoded_rows

        if as_dataframe is True:
            return _to_dataframe(decoded_rows)

        # Auto-detect: return DataFrame when pandas is installed.
        try:
            import pandas as pd  # type: ignore

            return pd.DataFrame(decoded_rows)
        except ModuleNotFoundError:
            return decoded_rows

    def get_schema(self, name: Identifier) -> list[ColumnDef]:
        return list(self._load_schema(name))

    def drop_dataset(self, name: Identifier) -> None:
        if not self._dataset_exists(name):
            return
        cur = self._conn.cursor()
        cur.execute(f"DROP TABLE {_quote(name)}")
        cur.execute(
            "DELETE FROM __dataset_schema__ WHERE dataset = ?",  # type: ignore[str-format]
            (name,),
        )
        self._conn.commit()

    # Internal helpers ------------------------------------------------
    def _init_metadata(self) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS __dataset_schema__ (
                dataset TEXT NOT NULL,
                column_name TEXT NOT NULL,
                declared_type TEXT NOT NULL,
                is_primary_key INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (dataset, column_name)
            )
            """
        )
        self._conn.commit()

    def _dataset_exists(self, name: Identifier) -> bool:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (name,),
        )
        return cur.fetchone() is not None

    def _persist_schema(self, dataset: Identifier, columns: Sequence[ColumnDef]) -> None:
        cur = self._conn.cursor()
        cur.executemany(
            """
            INSERT OR REPLACE INTO __dataset_schema__ (dataset, column_name, declared_type, is_primary_key)
            VALUES (:dataset, :column_name, :declared_type, :is_primary_key)
            """,
            [
                {
                    "dataset": dataset,
                    "column_name": col.name,
                    "declared_type": col.declared_type,
                    "is_primary_key": 1 if col.is_primary_key else 0,
                }
                for col in columns
            ],
        )

    def _load_schema(self, name: Identifier) -> Sequence[ColumnDef]:
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT column_name, declared_type, is_primary_key
            FROM __dataset_schema__
            WHERE dataset = ?
            ORDER BY rowid
            """,
            (name,),
        )
        rows = cur.fetchall()
        if not rows:
            raise DatasetNotFoundError(name)
        return [
            ColumnDef(row["column_name"], row["declared_type"], bool(row["is_primary_key"]))
            for row in rows
        ]

    def _normalize_schema(
        self,
        schema: Mapping[Identifier, str],
        primary_keys: Sequence[Identifier] | None,
    ) -> list[ColumnDef]:
        columns: list[ColumnDef] = []
        primary_set = set(primary_keys or [])

        for name, declared_type in schema.items():
            declared_upper = declared_type.upper()
            if declared_upper not in {"TEXT", "INTEGER", "REAL", "BLOB", "TIMESTAMP"}:
                raise SchemaError(f"Unsupported type {declared_type!r} for column {name!r}")
            columns.append(
                ColumnDef(
                    name=name,
                    declared_type=declared_upper,
                    is_primary_key=name in primary_set,
                )
            )

        missing_pks = primary_set - {c.name for c in columns}
        if missing_pks:
            raise SchemaError(f"Primary keys {sorted(missing_pks)} not present in schema")
        return columns

    @staticmethod
    def _infer_schema_from_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
        schema: dict[str, str] = {}
        for row in rows:
            for key, value in row.items():
                if key in schema:
                    continue
                schema[key] = _canonical_type(value)
        return schema


def _to_dataframe(rows: list[Mapping[str, Any]]):
    import pandas as pd  # type: ignore

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
