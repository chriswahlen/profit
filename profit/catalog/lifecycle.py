from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from profit.catalog.types import InstrumentRecord
from profit.catalog.store import CatalogStore
from profit.sources.types import LifecycleReader


@dataclass(frozen=True)
class LifecycleIntersection:
    requested_start: datetime
    requested_end: datetime
    active_start: datetime
    active_end: datetime | None
    clipped_start: datetime
    clipped_end: datetime

    @property
    def is_full(self) -> bool:
        return self.clipped_start == self.requested_start and self.clipped_end == self.requested_end

    @property
    def is_empty(self) -> bool:
        return self.clipped_start > self.clipped_end


def intersect_window(record: InstrumentRecord, start: datetime, end: datetime) -> LifecycleIntersection:
    """
    Intersect a requested window with the instrument's active interval.

    active_end is treated as open-ended when None.
    """
    if start > end:
        raise ValueError("start must be <= end")

    active_start = record.active_from
    active_end = record.active_to
    ae = active_end or end  # use requested end when open-ended for comparison

    clipped_start = max(start, active_start)
    clipped_end = min(end, ae)

    return LifecycleIntersection(
        requested_start=start,
        requested_end=end,
        active_start=active_start,
        active_end=active_end,
        clipped_start=clipped_start,
        clipped_end=clipped_end,
    )


def lookup_and_clip(
    store: CatalogStore,
    *,
    provider: str,
    provider_code: str,
    start: datetime,
    end: datetime,
    hard_fail: bool = True,
) -> LifecycleIntersection | None:
    """
    Convenience: fetch record from catalog and intersect window.

    Returns None if record not found. Raises ValueError if the intersection is empty and
    hard_fail is True; otherwise returns the empty intersection so caller can decide.
    """
    rec = store.get_instrument(provider=provider, provider_code=provider_code)
    if rec is None:
        return None
    li = intersect_window(rec, start, end)
    if li.is_empty and hard_fail:
        raise ValueError(
            f"Requested window {start.date()}–{end.date()} is outside lifecycle "
            f"{rec.active_from.date()}–{(rec.active_to.date() if rec.active_to else 'open')}"
        )
    return li


class CatalogLifecycleReader(LifecycleReader):
    """
    LifecycleReader backed by CatalogStore.
    """

    def __init__(self, store: CatalogStore) -> None:
        self.store = store

    def get_lifecycle(self, provider: str, provider_code: str) -> Optional[tuple[datetime, Optional[datetime]]]:
        rec = self.store.get_instrument(provider=provider, provider_code=provider_code)
        if rec is None:
            return None
        return rec.active_from, rec.active_to
