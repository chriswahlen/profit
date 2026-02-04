from __future__ import annotations

from typing import Optional

from profit.catalog.entity_store import EntityStore
from profit.sources.edgar.common import normalize_cik


def resolve_entity_id_from_identifier(store: EntityStore, identifier: str) -> Optional[str]:
    candidate = (identifier or "").strip()
    if not candidate:
        return None
    entity_id = store.resolve_entity_id(candidate)
    if entity_id:
        return entity_id
    if "|" in candidate:
        ticker = candidate.split("|", 1)[1].strip()
        if ticker:
            entity_id = store.resolve_entity_id(ticker)
            if entity_id:
                return entity_id
    return None


def resolve_cik_from_identifier(store: EntityStore, identifier: str) -> Optional[str]:
    candidate = (identifier or "").strip()
    if not candidate:
        return None
    entity_id = resolve_entity_id_from_identifier(store, candidate)
    if entity_id:
        cik = store.resolve_identifier(entity_id, "sec:cik", provider_id="sec:edgar")
        if cik:
            try:
                return normalize_cik(cik)
            except ValueError:
                pass
    try:
        return normalize_cik(candidate)
    except ValueError:
        pass
    if candidate.lower().startswith("cik:"):
        _, rest = candidate.split(":", 1)
        try:
            return normalize_cik(rest)
        except ValueError:
            pass
    return None
