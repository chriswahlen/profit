from __future__ import annotations

from typing import Iterable, Protocol, Sequence
from datetime import datetime

class CoverageAdapter(Protocol):
    """
    Adapter that lets BaseFetcher skip network calls when data already exists.
    """

    def get_unfetched_ranges(self, start: datetime, end: datetime) -> Sequence[tuple[datetime, datetime]]:
        ...

    def write_points(self, payload) -> None:
        ...

    def read_points(self, start: datetime, end: datetime):
        ...
