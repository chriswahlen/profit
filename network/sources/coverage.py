from __future__ import annotations

from datetime import datetime
from typing import Protocol, Sequence, Tuple


class CoverageAdapter(Protocol):
    """Thin interface so BaseFetcher can introspect what data it already has."""

    def get_unfetched_ranges(self, start: datetime, end: datetime) -> Sequence[Tuple[datetime, datetime]]:
        ...

    def write_points(self, payload) -> None:
        ...

    def read_points(self, start: datetime, end: datetime):
        ...
