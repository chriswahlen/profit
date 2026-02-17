from __future__ import annotations

import json
import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from data_sources.edgar.common import normalize_cik

logger = logging.getLogger(__name__)

_CIK_MAIN_RE = re.compile(r"^CIK(?P<cik>[0-9]{10})\.json$")
_CIK_PAGE_RE = re.compile(r"^CIK(?P<cik>[0-9]{10})-submissions-(?P<page>[0-9]{3})\.json$")


@dataclass(frozen=True)
class SubmissionsZipEntry:
    payload: Mapping[str, Any]
    # Best-effort timestamp derived from zip member metadata (UTC).
    fetched_at: datetime


def read_submissions_from_zip(zip_path: Path, cik: str | int) -> list[SubmissionsZipEntry]:
    """Read the base submissions JSON plus any paged filings JSON from a bulk zip."""
    cik_norm = normalize_cik(cik)
    main_name = f"CIK{cik_norm}.json"

    pages: list[tuple[int, str]] = []
    entries: list[SubmissionsZipEntry] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())
        if main_name not in names:
            return []

        entries.append(_read_zip_json(zf, main_name))

        for name in names:
            m = _CIK_PAGE_RE.match(name)
            if not m or m.group("cik") != cik_norm:
                continue
            pages.append((int(m.group("page")), name))

        for _, name in sorted(pages):
            entries.append(_read_zip_json(zf, name))

    return entries


def _read_zip_json(zf: zipfile.ZipFile, name: str) -> SubmissionsZipEntry:
    raw = zf.read(name)
    payload = json.loads(raw.decode("utf-8"))
    info = zf.getinfo(name)
    # Zip timestamps have no timezone; treat as UTC for reproducible ingestion logs.
    fetched_at = datetime(*info.date_time, tzinfo=timezone.utc)
    return SubmissionsZipEntry(payload=payload, fetched_at=fetched_at)
