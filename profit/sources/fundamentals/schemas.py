from __future__ import annotations

from profit.cache import SqliteStore


def ensure_sec_fundamentals_schemas(store: SqliteStore) -> None:
    """
    Ensure the SEC fundamentals datasets exist (idempotent).
    """
    _ensure_filing_schema(store)
    _ensure_fact_schema(store)
    _ensure_indexes(store)


def _ensure_filing_schema(store: SqliteStore) -> None:
    store.create_dataset(
        "fundamentals_filing:sec:v1",
        {
            "provider": "TEXT",
            "provider_code": "TEXT",
            "instrument_id": "TEXT",
            "accession": "TEXT",
            "form": "TEXT",
            "filed_at": "TIMESTAMP",
            "accepted_at": "TIMESTAMP",
            "known_at": "TIMESTAMP",
            "report_period_end": "TIMESTAMP",
            "is_amendment": "INTEGER",
            "asof": "TIMESTAMP",
            "attrs": "TEXT",
        },
        primary_keys=["provider", "provider_code", "accession"],
        if_not_exists=True,
    )


def _ensure_fact_schema(store: SqliteStore) -> None:
    store.create_dataset(
        "fundamentals_fact:sec:v1",
        {
            # identity / lineage
            "instrument_id": "TEXT",
            "provider": "TEXT",
            "provider_code": "TEXT",
            "accession": "TEXT",
            "form": "TEXT",
            "filed_at": "TIMESTAMP",
            "accepted_at": "TIMESTAMP",
            "known_at": "TIMESTAMP",
            "asof": "TIMESTAMP",
            # fact keys
            "tag_qname": "TEXT",
            "period_start": "TIMESTAMP",
            "period_end": "TIMESTAMP",
            "unit": "TEXT",
            "currency": "TEXT",
            # dimensions
            "dims_json": "TEXT",
            "dims_key": "TEXT",
            "dims_hash": "TEXT",
            # value
            "value_kind": "TEXT",
            "value_num": "REAL",
            "value_text_preview": "TEXT",
            "value_text_gz": "BLOB",
            "value_text_len": "INTEGER",
            "value_text_truncated": "INTEGER",
            # optional enrichment
            "statement": "TEXT",
            "line_item_code": "TEXT",
            "decimals": "INTEGER",
            "attrs": "TEXT",
        },
        primary_keys=[
            "instrument_id",
            "accession",
            "tag_qname",
            "period_start",
            "period_end",
            "unit",
            "dims_hash",
            "value_kind",
        ],
        if_not_exists=True,
    )


def _ensure_indexes(store: SqliteStore) -> None:
    cur = store._conn.cursor()
    cur.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_fundsec_filing_known_at
            ON "fundamentals_filing:sec:v1"(provider, provider_code, known_at);

        CREATE INDEX IF NOT EXISTS idx_fundsec_fact_known_at
            ON "fundamentals_fact:sec:v1"(instrument_id, known_at);

        CREATE INDEX IF NOT EXISTS idx_fundsec_fact_tag_period
            ON "fundamentals_fact:sec:v1"(instrument_id, tag_qname, period_end);

        CREATE INDEX IF NOT EXISTS idx_fundsec_fact_identity_known
            ON "fundamentals_fact:sec:v1"(
                instrument_id, tag_qname, period_start, period_end, unit, dims_hash, value_kind, known_at
            );

        CREATE INDEX IF NOT EXISTS idx_fundsec_fact_dims
            ON "fundamentals_fact:sec:v1"(dims_hash);
        """
    )
    store._conn.commit()
