from __future__ import annotations

import io
import zipfile
from typing import Dict

from data_sources.edgar.common import should_skip_accession_file


def expand_zip_archive(accession: str, payload: bytes) -> Dict[str, bytes]:
    """Return `file_name -> bytes` entries inside an EDGAR zip archive."""
    bio = io.BytesIO(payload)
    if not zipfile.is_zipfile(bio):
        return {}

    bio.seek(0)
    with zipfile.ZipFile(bio, "r") as zin:
        extracted: Dict[str, bytes] = {}
        for info in zin.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if should_skip_accession_file(accession, name):
                continue
            extracted[name] = zin.read(name)
    return extracted

