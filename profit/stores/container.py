from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from profit.cache.columnar_store import ColumnarSqliteStore
from profit.catalog.entity_store import EntityStore
from profit.catalog.store import CatalogStore
from profit.stores.redfin_store import RedfinStore

logger = logging.getLogger(__name__)


def _configure_shared_conn(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Apply connection-wide pragmas suitable for shared use across stores."""
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


@dataclass(frozen=True)
class StoreContainer:
    """
    Bundle of stores sharing a single SQLite connection.

    Using one connection ensures consistent transactional ordering across
    EntityStore, CatalogStore, and ColumnarSqliteStore while keeping WAL mode
    enabled for concurrent reads.
    """

    conn: sqlite3.Connection
    entity: EntityStore
    catalog: CatalogStore
    columnar: ColumnarSqliteStore
    redfin: RedfinStore
    _owns_conn: bool = False

    @classmethod
    def open(cls, db_path: Path, *, redfin_db_path: Path | None = None) -> "StoreContainer":
        """
        Open (or create) the database at ``db_path`` and return a container with
        all three stores sharing the same connection.
        """
        db_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_path = db_path.resolve()
        logger.info("opening store at %s", resolved_path)
        conn = sqlite3.connect(db_path)
        _configure_shared_conn(conn)
        entity = EntityStore(db_path, conn=conn)
        catalog = CatalogStore(db_path, conn=conn)
        columnar = ColumnarSqliteStore(conn=conn)
        if redfin_db_path is None:
            redfin = RedfinStore(db_path, conn=conn)
        else:
            logger.info("opening redfin store at %s", redfin_db_path.resolve())
            redfin_db_path.parent.mkdir(parents=True, exist_ok=True)
            redfin_conn = sqlite3.connect(redfin_db_path)
            _configure_shared_conn(redfin_conn)
            redfin = RedfinStore(redfin_db_path, conn=redfin_conn, owns_conn=True)
            redfin_db_path.parent.mkdir(parents=True, exist_ok=True)
            redfin_conn = sqlite3.connect(redfin_db_path)
            _configure_shared_conn(redfin_conn)
            redfin = RedfinStore(redfin_db_path, conn=redfin_conn, owns_conn=True)
        return cls(
            conn=conn,
            entity=entity,
            catalog=catalog,
            columnar=columnar,
            redfin=redfin,
            _owns_conn=True,
        )

    def close(self) -> None:
        self.redfin.close()
        if self._owns_conn:
            self.conn.close()
