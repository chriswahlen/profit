from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Sequence

from profit.config import get_cache_root


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


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_:]*$")


def _quote(identifier: Identifier) -> str:
    if not _IDENT_RE.match(identifier):
        raise SchemaError(f"Invalid identifier: {identifier!r}")
    return f'"{identifier}"'


def _default_db_path() -> Path:
    return get_cache_root() / "cache.sqlite3"


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

    def query(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None, *, as_dataframe: bool | None = None):
        """
        Run an arbitrary SELECT and decode TIMESTAMP columns similarly to `read`.

        Intended for advanced queries (CTEs, window functions) that `read` does not support.
        """
        cur = self._conn.cursor()
        cur.execute(sql, params or [])
        rows = cur.fetchall()
        if not rows:
            return [] if as_dataframe is False else []

        # Best-effort decode: use cursor description to identify TIMESTAMP columns
        # by matching the column name to the dataset schema when possible.
        # When schema is unknown (arbitrary query), we fall back to raw values.
        col_names = [desc[0] for desc in cur.description]
        decoded_rows = []
        for row in rows:
            as_dict = {}
            for idx, name in enumerate(col_names):
                val = row[idx]
                if isinstance(val, str) and _looks_like_iso_ts(val):
                    try:
                        as_dict[name] = _decode_value(val, "TIMESTAMP")
                        continue
                    except Exception:
                        pass
                as_dict[name] = val
            decoded_rows.append(as_dict)

        if as_dataframe is False:
            return decoded_rows
        if as_dataframe is True:
            try:
                import pandas as pd  # type: ignore

                return pd.DataFrame(decoded_rows)
            except ModuleNotFoundError:
                return decoded_rows
        try:
            import pandas as pd  # type: ignore

            return pd.DataFrame(decoded_rows)
        except ModuleNotFoundError:
            return decoded_rows

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
        self._conn.commit()

    def _load_schema(self, name: Identifier) -> list[ColumnDef]:
        if not self._dataset_exists(name):
            raise DatasetNotFoundError(name)
        cur = self._conn.cursor()
        cur.execute(
            """
            SELECT column_name, declared_type, is_primary_key
            FROM __dataset_schema__
            WHERE dataset = ?
            ORDER BY rowid ASC
            """,
            (name,),
        )
        rows = cur.fetchall()
        return [ColumnDef(r["column_name"], r["declared_type"], bool(r["is_primary_key"])) for r in rows]

    def _normalize_schema(
        self,
        schema: Mapping[Identifier, str],
        primary_keys: Sequence[Identifier] | None = None,
    ) -> list[ColumnDef]:
        primary_keys = list(primary_keys or [])
        columns = []
        for name, decl in schema.items():
            if not _IDENT_RE.match(name):
                raise SchemaError(f"Invalid column name: {name!r}")
            col = ColumnDef(name, decl.upper(), name in primary_keys)
            columns.append(col)
        if primary_keys:
            missing = set(primary_keys) - {c.name for c in columns}
            if missing:
                raise SchemaError(f"Primary key columns missing from schema: {missing}")
        return columns

    def _infer_schema_from_rows(self, rows: Iterable[Mapping[str, Any]]) -> dict[Identifier, str]:
        inferred: dict[Identifier, str] = {}
        for row in rows:
            for k, v in row.items():
                decl = _canonical_type(v)
                prev = inferred.get(k)
                if prev is None:
                    inferred[k] = decl
                elif prev != decl:
                    raise SchemaError(f"Conflicting inferred types for column {k!r}: {prev} vs {decl}")
        return inferred


def _looks_like_iso_ts(val: str) -> bool:
    # Cheap check for ISO8601 with Z or offset.
    return "T" in val and ("Z" in val or "+" in val or "-" in val)

def _to_dataframe(rows: list[Mapping[str, Any]]):
    import pandas as pd  # type: ignore

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
