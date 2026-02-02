from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from profit.sources.redfin_dump_ingest import (
    RedfinIngestConfig,
    RedfinIngestionStats,
    ingest_redfin_rows,
    record_ingestion_run,
    rows_from_dump,
)
from profit.stores import StoreContainer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Redfin data from local downloads and load it into the Redfin store")
    parser.add_argument(
        "--downloads-dir",
        type=Path,
        default=Path("data/datasets/redfin"),
        help="Directory containing Redfin dump files (default data/datasets/redfin)",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help="Specific filename under downloads-dir to load; if omitted uses the most recent TSV/CSV",
    )
    parser.add_argument("--store-path", type=Path, default=Path("data/profit.sqlite"), help="Columnar store path")
    parser.add_argument(
        "--redfin-db-path",
        type=Path,
        default=Path("data/redfin.sqlite"),
        help="SQLite file for the Redfin store (default data/redfin.sqlite)",
    )
    parser.add_argument("--granularity", choices=("day", "week", "month"), default="week", help="Period granularity (default week)")
    parser.add_argument("--country-iso2", default="US", help="Country ISO2 for fallback regions")
    parser.add_argument("--data-revision", type=int, default=0, help="Default data_revision for rows lacking a value")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on the number of rows to process (for testing)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args()


def _find_latest_file(directory: Path) -> Path | None:
    if not directory.exists():
        return None
    candidates = sorted(
        directory.glob("*.tsv*"),
        key=lambda path: path.stat().st_mtime,
    )
    return candidates[-1] if candidates else None


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    downloads_dir = args.downloads_dir.expanduser()
    if not downloads_dir.exists():
        logging.error("downloads directory %s does not exist", downloads_dir)
        sys.exit(1)

    if args.dataset:
        dump_path = downloads_dir / args.dataset
        if not dump_path.exists():
            logging.error("dataset %s not found under %s", args.dataset, downloads_dir)
            sys.exit(1)
    else:
        dump_path = _find_latest_file(downloads_dir)
        if dump_path is None:
            logging.error("no TSV/CSV dumps found under %s", downloads_dir)
            sys.exit(1)

    store_path = args.store_path.resolve()
    store_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("loading dataset %s", dump_path.name)
    logging.info("columnar store path: %s", store_path)
    logging.info("redfin store path: %s", args.redfin_db_path.resolve() if args.redfin_db_path else "unspecified")
    if args.limit:
        logging.info("ingestion limit enabled (%s rows)", args.limit)
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
        source_url=str(dump_path),
    )

    stats: RedfinIngestionStats | None = None
    status = "failed"
    notes: str | None = None
    rows = rows_from_dump(dump_path, delimiter="\t", limit=args.limit)

    logging.info("starting ingestion run %s", run_id)
    try:
        stats = ingest_redfin_rows(conn=stores.redfin.conn, rows=rows, config=config, run_started_at=started_at)
        logging.info(
            "ingested %s rows for %s regions (max revision %s)",
            stats.row_count,
            stats.regions,
            stats.max_data_revision,
        )
        status = "success"
    except Exception as exc:
        notes = f"ingestion failed: {exc}"
        logging.exception("fetch ingestion failed")
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
            source_url=str(dump_path),
            row_count=stats.row_count if stats else 0,
            data_revision=stats.max_data_revision if stats else config.default_data_revision,
            notes=notes,
        )
        stores.redfin.conn.commit()
        stores.close()


if __name__ == "__main__":
    main()
