from __future__ import annotations

from datetime import datetime
from typing import Optional, Protocol, Tuple


class Fingerprintable(Protocol):
    """Represents something that can produce a deterministic cache key."""

    def fingerprint(self) -> str:
        ...


class LifecycleReader(Protocol):
    """Provides lifecycle bounds for provider instruments (unused for EDGAR)."""

    def get_lifecycle(self, provider: str, provider_code: str) -> Optional[Tuple[datetime, Optional[datetime]]]:
        ...
