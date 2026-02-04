#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from profit.catalog import EntityStore
from profit.catalog.identifier_utils import resolve_cik_from_identifier
from profit.config import ProfitConfig
from profit.edgar import EdgarDatabase
from profit.sources.edgar.common import normalize_cik


@dataclass(frozen=True)
class EdgarFileInfo:
    file_name: str
    fetched_at: str | None
    source_url: str | None
    has_payload: bool
    payload_bytes: int | None


@dataclass(frozen=True)
class EdgarAccessionSummary:
    accession: str
    base_url: str | None
    file_count: int
    fetched_at: str | None
    files: tuple[EdgarFileInfo, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Describe the locally cached SEC EDGAR assets for a company."
    )
    parser.add_argument(
        "identifier",
        help="CIK (e.g. 0000320193 or CIK:0000320193), ticker, or catalog identifier to resolve.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        help="Override the profit data root (defaults to PROFIT_DATA_ROOT or ~/.profit.conf).",
    )
    parser.add_argument(
        "--profit-db",
        type=Path,
        help="Override the path to the catalog database (default data_root/profit.sqlite).",
    )
    parser.add_argument(
        "--edgar-db",
        type=Path,
        help="Override the path to the edgar assets database (default data_root/edgar.sqlite3).",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help="Directory where the EDGAR files should be exported after listing.",
    )
    return parser.parse_args()


def _first_existing(description: str, candidates: Iterable[Path]) -> Path:
    expanded = [candidate.expanduser() for candidate in candidates]
    for candidate in expanded:
        if candidate.exists():
            return candidate
    raise SystemExit(
        f"{description} not found; checked {', '.join(str(p) for p in expanded)}"
    )


def resolve_db_paths(
    data_root: Path,
    profit_override: Path | None,
    edgar_override: Path | None,
) -> tuple[Path, Path]:
    root = data_root.expanduser()
    if not root.exists():
        raise SystemExit(f"{root} does not exist")
    if not root.is_dir():
        raise SystemExit(f"{root} is not a directory; pass the data root folder")

    profit_candidates: list[Path] = [profit_override] if profit_override else []
    if not profit_override:
        profit_candidates.extend(
            [
                root / "profit.sqlite",
                root / "profit.sqlite3",
                root / "profit.db",
            ]
        )
    edgar_candidates: list[Path] = [edgar_override] if edgar_override else []
    if not edgar_override:
        edgar_candidates.extend(
            [
                root / "edgar.sqlite3",
                root / "edgar.sqlite",
            ]
        )

    profit_db = _first_existing("profit catalog", profit_candidates)
    edgar_db = _first_existing("edgar database", edgar_candidates)
    return profit_db, edgar_db


def _resolve_cik(identifier: str, profit_db_path: Path) -> str:
    store = EntityStore(profit_db_path, readonly=True)
    try:
        cik = resolve_cik_from_identifier(store, identifier)
    finally:
        store.close()
    if not cik:
        raise SystemExit(
            f"Unable to resolve a CIK for identifier {identifier!r} using {profit_db_path}"
        )
    return cik


def _entity_name_for_cik(db: EdgarDatabase, cik: str) -> str | None:
    cur = db.conn.execute(
        "SELECT entity_name FROM edgar_submissions WHERE cik = ? ORDER BY fetched_at DESC LIMIT 1",
        (normalize_cik(cik),),
    )
    row = cur.fetchone()
    return row["entity_name"] if row else None


def _files_for_accession(db: EdgarDatabase, accession: str) -> tuple[EdgarFileInfo, ...]:
    cur = db.conn.execute(
        """
        SELECT
            file_name,
            fetched_at,
            source_url,
            compressed_payload IS NOT NULL AS has_payload,
            LENGTH(compressed_payload) AS payload_bytes
        FROM edgar_accession_file
        WHERE accession = ?
        ORDER BY file_name
        """,
        (accession,),
    )
    rows = cur.fetchall()
    return tuple(
        EdgarFileInfo(
            file_name=row["file_name"],
            fetched_at=row["fetched_at"],
            source_url=row["source_url"],
            has_payload=bool(row["has_payload"]),
            payload_bytes=row["payload_bytes"],
        )
        for row in rows
    )


def _accessions_for_cik(db: EdgarDatabase, cik: str) -> list[EdgarAccessionSummary]:
    cur = db.conn.execute(
        """
        SELECT accession, base_url, file_count, fetched_at
        FROM edgar_accession
        WHERE cik = ?
        ORDER BY accession
        """,
        (normalize_cik(cik),),
    )
    return [
        EdgarAccessionSummary(
            accession=row["accession"],
            base_url=row["base_url"],
            file_count=row["file_count"],
            fetched_at=row["fetched_at"],
            files=_files_for_accession(db, row["accession"]),
        )
        for row in cur.fetchall()
    ]


def _format_bytes(size: int | None) -> str:
    if size is None:
        return "n/a"
    return f"{size} B"


def _print_summary(cik: str, entity_name: str | None, accessions: list[EdgarAccessionSummary]) -> None:
    print(f"CIK: {cik}")
    print(f"Entity: {entity_name or 'unknown'}")
    if not accessions:
        print("No accessions recorded in edgar.sqlite3 for this CIK.")
        return
    print("Accessions:")
    for accession in accessions:
        metadata = [f"files={len(accession.files)}", f"index_count={accession.file_count}"]
        if accession.base_url:
            metadata.append(f"base_url={accession.base_url}")
        if accession.fetched_at:
            metadata.append(f"fetched_at={accession.fetched_at}")
        print(f"- {accession.accession} ({', '.join(metadata)})")
        if not accession.files:
            print("    (no files stored for this accession)")
            continue
        for file_info in accession.files:
            file_meta = [f"stored={file_info.has_payload}", f"size={_format_bytes(file_info.payload_bytes)}"]
            if file_info.fetched_at:
                file_meta.append(f"fetched_at={file_info.fetched_at}")
            if file_info.source_url:
                file_meta.append(f"url={file_info.source_url}")
            print(f"    {file_info.file_name} ({', '.join(file_meta)})")


def _export_accessions(
    target_root: Path, db: EdgarDatabase, accessions: list[EdgarAccessionSummary]
) -> tuple[int, int]:
    target_root = target_root.expanduser()
    target_root.mkdir(parents=True, exist_ok=True)
    exported = 0
    missing = 0
    for accession in accessions:
        base_dir = target_root / accession.accession
        base_dir.mkdir(parents=True, exist_ok=True)
        for file_info in accession.files:
            payload = db.get_file(accession.accession, file_info.file_name)
            if payload is None:
                missing += 1
                continue
            dest_path = base_dir / file_info.file_name
            dest_path.write_bytes(payload)
            exported += 1
    return exported, missing


def main() -> None:
    args = parse_args()
    data_root = args.data_root or ProfitConfig.resolve_data_root()
    profit_db, edgar_db = resolve_db_paths(
        data_root, args.profit_db, args.edgar_db
    )
    cik = _resolve_cik(args.identifier, profit_db)
    db = EdgarDatabase(edgar_db)
    try:
        entity_name = _entity_name_for_cik(db, cik)
        accessions = _accessions_for_cik(db, cik)
        _print_summary(cik, entity_name, accessions)
        if args.output_path:
            exported, missing = _export_accessions(args.output_path, db, accessions)
            status = f"{exported} files exported to {args.output_path.expanduser()}"
            if missing:
                status += f" ({missing} files missing payloads in the database)"
            print(status)
    finally:
        db.close()


if __name__ == "__main__":
    main()
