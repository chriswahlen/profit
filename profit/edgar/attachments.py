from __future__ import annotations

from pathlib import Path


ATTACHMENT_SUFFIXES = {".pdf", ".xlsx", ".xslx", ".xls"}


def is_attachment_filename(name: str) -> bool:
    """Return True if the file should be exported to disk when requested."""
    if not name:
        return False
    return Path(name).suffix.lower() in ATTACHMENT_SUFFIXES


def save_attachment(name: str, payload: bytes | None, target_dir: Path) -> Path | None:
    """Write an attachment to ``target_dir`` if payload is present.

    The filename is sanitized to avoid directory traversal.
    """
    if payload is None or not is_attachment_filename(name):
        return None

    target_dir.mkdir(parents=True, exist_ok=True)
    safe_name = name.replace("/", "_").replace("\\", "_")
    path = target_dir / safe_name
    path.write_bytes(payload)
    return path

