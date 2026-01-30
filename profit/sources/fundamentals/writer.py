from __future__ import annotations

import gzip
from datetime import datetime, timezone
import json
from typing import Iterable, Mapping, Any

from profit.cache import SqliteStore
from profit.sources.fundamentals.schemas import ensure_sec_fundamentals_schemas


DEFAULT_TEXT_MAX_CHARS = 262_144  # 256 KiB
DEFAULT_TEXT_PREVIEW_CHARS = 512


def _to_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def encode_text(text: str, *, max_chars: int = DEFAULT_TEXT_MAX_CHARS, preview_chars: int = DEFAULT_TEXT_PREVIEW_CHARS):
    """
    Truncate, preview, and gzip a text fact.
    """
    orig_len = len(text)
    truncated = text[:max_chars]
    preview = truncated[:preview_chars]
    gz = gzip.compress(truncated.encode("utf-8"))
    return {
        "value_text_preview": preview,
        "value_text_gz": gz,
        "value_text_len": orig_len,
        "value_text_truncated": 1 if orig_len > max_chars else 0,
    }


def write_filings(store: SqliteStore, rows: Iterable[Mapping[str, Any]]) -> int:
    """
    Append filing rows; ensures schema exists.
    """
    ensure_sec_fundamentals_schemas(store)
    encoded = []
    for row in rows:
        base = dict(row)
        attrs = base.get("attrs")
        if isinstance(attrs, dict):
            base["attrs"] = json.dumps(attrs)
        encoded.append(base)
    return store.append("fundamentals_filing:sec:v1", encoded, overwrite=True)


def write_facts(
    store: SqliteStore,
    rows: Iterable[Mapping[str, Any]],
    *,
    max_text_chars: int = DEFAULT_TEXT_MAX_CHARS,
    preview_chars: int = DEFAULT_TEXT_PREVIEW_CHARS,
    overwrite: bool = True,
) -> int:
    """
    Append fact rows; ensures schema exists and applies text encoding rules.
    """
    ensure_sec_fundamentals_schemas(store)
    prepared = []
    for row in rows:
        value_kind = row.get("value_kind")
        if value_kind not in ("number", "text"):
            raise ValueError(f"value_kind must be 'number' or 'text', got {value_kind!r}")

        base = dict(row)
        # Normalize timestamps to UTC ISO strings via append encoder; just ensure tz-aware here.
        for key in ("filed_at", "accepted_at", "known_at", "asof", "period_start", "period_end"):
            if key in base:
                base[key] = _to_utc(base[key])  # type: ignore[arg-type]

        if value_kind == "text":
            raw_text = base.pop("value_text", None)
            if raw_text is None:
                raise ValueError("text fact requires value_text")
            enc = encode_text(str(raw_text), max_chars=max_text_chars, preview_chars=preview_chars)
            base.update(enc)
            base["value_num"] = None
        else:
            if "value_num" not in base:
                raise ValueError("numeric fact requires value_num")
            base.setdefault("value_text_preview", None)
            base.setdefault("value_text_gz", None)
            base.setdefault("value_text_len", None)
            base.setdefault("value_text_truncated", None)
        attrs = base.get("attrs")
        if isinstance(attrs, dict):
            base["attrs"] = json.dumps(attrs)
        prepared.append(base)

    return store.append("fundamentals_fact:sec:v1", prepared, overwrite=overwrite)
