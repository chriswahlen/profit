from __future__ import annotations

from pathlib import Path

from profit.stores import StoreContainer


def test_store_container_initializes_shared_schema(tmp_path):
    db_path = tmp_path / "profit.sqlite"
    container = StoreContainer.open(db_path)

    # Entity schema
    providers = container.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='provider'"
    ).fetchone()
    assert providers is not None

    # Catalog schema
    instruments = container.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='instrument'"
    ).fetchone()
    assert instruments is not None

    # Columnar schema
    col_series = container.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='__col_series__'"
    ).fetchone()
    assert col_series is not None

    # All stores share the same connection object.
    assert container.entity.conn is container.catalog.conn is container.columnar._conn  # type: ignore[attr-defined]

    container.close()
    assert db_path.exists()
