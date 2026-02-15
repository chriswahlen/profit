from __future__ import annotations

import sqlite3
import logging
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
        self._logged_open = False
        self._logger = logging.getLogger(__name__)

    # Lazily open to avoid doing work when only describing the source.
    def _ensure_conn(self) -> sqlite3.Connection:
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            # Enforce FK constraints for reliable schemas.
            self.conn.execute("PRAGMA foreign_keys = ON;")
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
        if not self._logged_open:
            self._logger.info("Opened sqlite database %s", self.db_path)
            self._logged_open = True
        return self.conn

    @property
    def connection(self) -> sqlite3.Connection:
        """Expose the lazily initialized connection for batch operations."""
        return self._ensure_conn()

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
