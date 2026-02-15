from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from config import Config
from data_sources.data_store import DataSourceStore


class SqliteDataStore(DataSourceStore, ABC):
    """Shared helpers for SQLite-backed data stores."""

    def __init__(self, *, db_name: str, config: Config, summary: str):
        self.summary = summary
        self.config = config
        data_dir = Path(self.config.data_path())
        data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = data_dir / db_name
        self.conn: Optional[sqlite3.Connection] = None

    # Lazily open to avoid doing work when only describing the source.
    def _ensure_conn(self) -> sqlite3.Connection:
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            # Enforce FK constraints for reliable schemas.
            self.conn.execute("PRAGMA foreign_keys = ON;")
        return self.conn

    @abstractmethod
    def describe_brief(self) -> str:
        raise NotImplementedError

    def describe_detailed(self, *, indent: str = "  ") -> str:
        conn = self._ensure_conn()
        cur = conn.cursor()
        cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name;")
        rows = cur.fetchall()
        if not rows:
            return f"{indent}No tables yet. Expected schema will be created on first ingest."

        lines = []
        for name, ddl in rows:
            lines.append(f"{indent}{name}: {ddl}")
        return "\n".join(lines)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
