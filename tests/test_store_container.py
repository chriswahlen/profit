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

    # Redfin schema
    regions = container.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='regions'"
    ).fetchone()
    assert regions is not None

    # All stores share the same connection object.
    assert (
        container.entity.conn
        is container.catalog.conn
        is container.columnar._conn  # type: ignore[attr-defined]
        is container.redfin.conn
    )

    container.close()


def test_store_container_can_use_separate_redfin_db(tmp_path):
    db_path = tmp_path / "profit.sqlite"
    redfin_db = tmp_path / "redfin.sqlite"
    container = StoreContainer.open(db_path, redfin_db_path=redfin_db)

    regions = container.redfin.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='regions'"
    ).fetchone()
    assert regions is not None
    assert redfin_db.exists()
    assert container.redfin.conn is not container.conn

    container.close()
    assert db_path.exists()
