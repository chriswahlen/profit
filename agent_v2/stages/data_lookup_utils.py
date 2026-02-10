from __future__ import annotations

from typing import Any, Mapping


def extract_datasets(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """
    Normalize the ``datasets`` list produced by a data lookup prompt.

    The caller is responsible for ensuring ``payload`` is a JSON object.
    """

    out: dict[str, dict[str, Any]] = {}
    raw = payload.get("datasets")
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        key = item.get("key")
        if not isinstance(key, str):
            continue
        stripped = key.strip()
        if not stripped:
            continue
        normalized = dict(item)
        rows = normalized.get("rows")
        if not isinstance(rows, list):
            rows = []
        normalized["rows"] = rows
        out[stripped] = normalized
    return out
