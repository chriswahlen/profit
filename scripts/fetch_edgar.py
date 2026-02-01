from __future__ import annotations

import argparse
import logging
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Optional

from profit.cache import FileCache
from profit.config import ProfitConfig, get_setting
from profit.edgar import EdgarDatabase
from profit.edgar.zip_utils import expand_zip_archive
from profit.edgar.xml_sanitizer import markdown_textblocks
from profit.sources.edgar import convert_html_to_markdown_bytes
from profit.sources.edgar import (
    EdgarSubmissionsFetcher,
    EdgarSubmissionsRequest,
    EdgarAccessionReader,
    should_skip_accession_file,
)
from profit.sources.types import LifecycleReader
from profit.catalog.refresher import CatalogChecker
from profit.utils.url_fetcher import PermanentFetchError


class _AlwaysActiveLifecycle(LifecycleReader):
    def get_lifecycle(self, provider: str, provider_code: str):
        # Accept any window; EDGAR submissions are effectively append-only.
        from datetime import datetime, timezone

        return datetime(1900, 1, 1, tzinfo=timezone.utc), None


class _NoopCatalogChecker(CatalogChecker):
    def __init__(self):
        # Dummy attributes to satisfy type expectations; not used.
        self.store = None
        self.refresher = None
        self.max_age = timedelta(days=9999)
        self.allow_network = True
        self.use_cache_only = False

    def ensure_fresh(self, provider: str):
        return

    def require_present(self, provider: str, provider_code: str):
        return


def _profit_cfg(args) -> ProfitConfig:
    data_root = args.data_root or ProfitConfig.resolve_data_root()
    cache_root = args.cache_root or ProfitConfig.resolve_cache_root()
    store_path = args.store_path or data_root / "profit.sqlite"
    return ProfitConfig(
        data_root=data_root,
        cache_root=cache_root,
        store_path=store_path,
        log_level=args.log_level,
        refresh_catalog=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch SEC EDGAR submissions for a CIK")
    parser.add_argument("cik", help="CIK (with or without leading zeros)")
    parser.add_argument(
        "--user-agent",
        help="User-Agent for SEC requests (falls back to PROFIT_SEC_USER_AGENT env)",
    )
    parser.add_argument(
        "--accession",
        help="Optional accession number (e.g., 0000320193-24-000001) to fetch index.json for this CIK",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        dest="cache_root",
        help="Override cache root (default PROFIT_CACHE_ROOT/*)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        help="Override data root (default PROFIT_DATA_ROOT)",
    )
    parser.add_argument(
        "--store-path",
        type=Path,
        help="Override store path (defaults to DATA_ROOT/profit.sqlite)",
    )
    parser.add_argument("--ttl-minutes", type=int, default=1440, help="Cache TTL minutes (default 1440 = 1 day)")
    parser.add_argument("--offline", action="store_true", help="Use cache only; skip network")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default INFO)")

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")

    cfg = _profit_cfg(args)
    cache = FileCache(base_dir=cfg.cache_root / "edgar_fetcher")
    edgar_db = EdgarDatabase(cfg.data_root / "edgar.sqlite3")
    ua = args.user_agent or get_setting("PROFIT_SEC_USER_AGENT")
    if not ua:
        edgar_db.close()
        raise SystemExit("User-Agent required: set --user-agent or PROFIT_SEC_USER_AGENT")

    try:
        fetcher = EdgarSubmissionsFetcher(
            cfg=cfg,
            cache=cache,
            ttl=timedelta(minutes=args.ttl_minutes),
            offline=args.offline,
            lifecycle=_AlwaysActiveLifecycle(),
            catalog_checker=_NoopCatalogChecker(),
            user_agent=ua,
        )

        req = EdgarSubmissionsRequest(args.cik)
        result = fetcher.fetch(req)
        edgar_db.record_submissions(result.cik, result.entity_name, result.raw)

        print(f"CIK: {result.cik}")
        print(f"Entity: {result.entity_name}")
        print(f"Recent filings: {len(result.recent_filings)}")
        for filing in result.recent_filings[:10]:
            report = filing.report_date.isoformat() if filing.report_date else "-"
            print(f"{filing.filing_date.isoformat()} {filing.form:6} {filing.accession_number} report={report} doc={filing.primary_document}")

        if args.accession:
            print("\nAccession index:")
            reader = EdgarAccessionReader(
                cache=cache,
                user_agent=ua,
                ttl=timedelta(minutes=args.ttl_minutes),
                allow_network=not args.offline,
            )
            try:
                acc = reader.fetch_index(args.cik, args.accession)
            except PermanentFetchError as exc:
                print(f"Accession fetch failed ({exc.status}): {exc.url}")
            else:
                file_names: list[str] = []
                for item in acc.files:
                    if isinstance(item, dict):
                        name = item.get("name")
                    else:
                        name = str(item)
                    if not name or should_skip_accession_file(args.accession, name):
                        logging.info("skipping %s", name or "<missing name>")
                        continue
                    file_names.append(name)
                    print(f"- {name}")
                edgar_db.record_accession_index(result.cik, args.accession, acc.base_url, file_names)

                existing_names = set(file_names)
                for name in file_names:
                    if edgar_db.has_file(args.accession, name):
                        continue
                    if name.lower().endswith(".zip"):
                        try:
                            payload = reader.fetch_file(args.cik, args.accession, name)
                        except PermanentFetchError as file_exc:
                            logging.warning("skipping zip file due to fetch error %s %s", name, file_exc)
                            continue
                        expanded = expand_zip_archive(args.accession, payload)
                        for entry_name, entry_payload in expanded.items():
                            already_seen = entry_name in existing_names or edgar_db.has_file(args.accession, entry_name)
                            if already_seen:
                                logging.info("dedup skipping %s (source=%s)", entry_name, name)
                                continue
                            if entry_name.lower().endswith(".xml"):
                                _debug_dump(args.accession, entry_name, entry_payload, "before")
                                entry_payload = markdown_textblocks(entry_payload)
                                _debug_dump(args.accession, entry_name, entry_payload, "after")
                            elif entry_name.lower().endswith((".htm", ".html")):
                                _debug_dump(args.accession, entry_name, entry_payload, "before_html")
                                entry_payload = convert_html_to_markdown_bytes(entry_name, entry_payload)
                                _debug_dump(args.accession, entry_name, entry_payload, "md")
                            edgar_db.store_file(args.accession, entry_name, entry_payload)
                            existing_names.add(entry_name)
                        # Do not store the raw zip; we keep expanded files only.
                        continue
                    try:
                        payload = reader.fetch_file(args.cik, args.accession, name)
                    except PermanentFetchError as file_exc:
                        logging.warning("skipping file due to fetch error %s %s", name, file_exc)
                        continue
                    if name.lower().endswith(".xml"):
                        _debug_dump(args.accession, name, payload, "before")
                        payload = markdown_textblocks(payload)
                        _debug_dump(args.accession, name, payload, "after")
                    elif name.lower().endswith((".htm", ".html")):
                        _debug_dump(args.accession, name, payload, "before_html")
                        payload = convert_html_to_markdown_bytes(name, payload)
                        _debug_dump(args.accession, name, payload, "md")
                    edgar_db.store_file(args.accession, name, payload)
    finally:
        edgar_db.close()


def _debug_dump(accession: str, name: str, payload: bytes, label: str) -> None:
    try:
        tmp_dir = Path(tempfile.gettempdir()) / "edgar_xml_debug"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        safe_name = name.replace("/", "_")
        path = tmp_dir / f"{accession}_{safe_name}.{label}"
        path.write_bytes(payload)
        logging.info("debug output path=%s", path)
    except Exception as exc:  # pragma: no cover - best-effort debug
        logging.warning("failed to write debug file for %s (%s)", name, exc)


if __name__ == "__main__":
    main()
