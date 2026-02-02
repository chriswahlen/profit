from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from profit.sources.redfin_dump_ingest import (
    RedfinIngestConfig,
    RedfinIngestionStats,
    ingest_redfin_rows,
    record_ingestion_run,
    rows_from_dump,
)
from profit.stores import StoreContainer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a Redfin data dump into the redfin store tables")
    parser.add_argument("dump_path", type=Path, help="Path to the Redfin dump (TSV/CSV or compressed .gz)")
    parser.add_argument("--store-path", type=Path, default=Path("data/profit.sqlite"), help="Path to the columnar store (default data/profit.sqlite)")
    parser.add_argument("--redfin-db-path", type=Path, default=None, help="Optional separate SQLite file for the Redfin store")
    parser.add_argument("--source-url", type=str, default=None, help="Report the URL where the dump was downloaded from")
    parser.add_argument("--granularity", choices=("day", "week", "month"), default="week", help="Period granularity for the dump (default: week)")
    parser.add_argument("--country-iso2", default="US", help="Country code to use for regions lacking explicit info (default US)")
    parser.add_argument("--data-revision", type=int, default=0, help="Default data_revision for rows without an explicit value")
    parser.add_argument("--delimiter", default="\t", help="Delimiter used in the dump (default tab)")
    parser.add_argument("--limit", type=int, default=None, help="Optional maximum number of rows to ingest (testing/debug)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default INFO)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    if not args.dump_path.exists():
        logging.error("dump_path %s does not exist", args.dump_path)
        sys.exit(1)

    store_path = args.store_path.resolve()
    store_path.parent.mkdir(parents=True, exist_ok=True)

    stores = StoreContainer.open(store_path, redfin_db_path=args.redfin_db_path)
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc)
    config = RedfinIngestConfig(
        provider="redfin",
        period_granularity=args.granularity,
        country_iso2=args.country_iso2,
        default_data_revision=args.data_revision,
        source_url=args.source_url,
    )

    stats: RedfinIngestionStats | None = None
    status = "failed"
    notes: str | None = None
    rows = rows_from_dump(args.dump_path, delimiter=args.delimiter, limit=args.limit)

    try:
        stats = ingest_redfin_rows(conn=stores.redfin.conn, rows=rows, config=config, run_started_at=started_at)
        logging.info("ingested %s rows for %s regions (revision %s)", stats.row_count, stats.regions, stats.max_data_revision)
        status = "success"
    except Exception as exc:  # pragma: no cover - propagate for callers
        notes = f"ingestion failed: {exc}"
        logging.exception("redfin dump ingestion failed")
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        record_ingestion_run(
            conn=stores.redfin.conn,
            run_id=run_id,
            provider=config.provider,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            source_url=config.source_url,
            row_count=stats.row_count if stats else 0,
            data_revision=stats.max_data_revision if stats else config.default_data_revision,
            notes=notes,
        )
        stores.close()


if __name__ == "__main__":
    main()
