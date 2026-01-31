from __future__ import annotations

import json
from datetime import datetime, timezone

from profit.edgar import EdgarDatabase


def test_edgar_db_records_submissions(tmp_path):
    db_path = tmp_path / "edgar.sqlite3"
    db = EdgarDatabase(db_path)
    payload = {"foo": "bar"}
    db.record_submissions("0000123456", "Example Inc.", payload, fetched_at=datetime(2024, 1, 1, tzinfo=timezone.utc))

    cur = db.conn.execute("SELECT cik, entity_name, fetched_at, payload FROM edgar_submissions")
    row = cur.fetchone()
    assert row["cik"] == "0000123456"
    assert row["entity_name"] == "Example Inc."
    assert row["fetched_at"].startswith("2024-01-01T")
    assert json.loads(row["payload"]) == payload

    db.close()


def test_edgar_db_records_accession_files(tmp_path):
    db_path = tmp_path / "edgar.sqlite3"
    db = EdgarDatabase(db_path)
    files = ["a.htm", "b.pdf"]
    db.record_accession_index(
        "0000123456",
        "0000123456-00-000001",
        "https://example.com/edgar/",
        files,
        fetched_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    row = db.conn.execute("SELECT base_url, file_count FROM edgar_accession").fetchone()
    assert row["base_url"] == "https://example.com/edgar/"
    assert row["file_count"] == 2

    fetched = db.get_accession_files("0000123456-00-000001")
    assert set(fetched) == set(files)

    payload = b"hello world"
    db.store_file("0000123456-00-000001", "a.htm", payload, fetched_at=datetime(2024, 1, 3, tzinfo=timezone.utc))
    assert db.has_file("0000123456-00-000001", "a.htm")
    assert db.get_file("0000123456-00-000001", "a.htm") == payload

    db.close()
