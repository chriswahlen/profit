from __future__ import annotations

from datetime import datetime
from typing import Protocol, Tuple, Optional


class Fingerprintable(Protocol):
    """
    Request objects must expose a stable fingerprint for cache keys.

    Fingerprints should be deterministic for semantically identical requests and
    should not include non-essential ordering differences.
    """

    def fingerprint(self) -> str:
        ...


class LifecycleReader(Protocol):
    """
    Provides lifecycle bounds for provider instruments.
    """

    def get_lifecycle(self, provider: str, provider_code: str) -> Optional[Tuple[datetime, Optional[datetime]]]:
        """
        Return (active_from, active_to) or None if unknown.
        """
        ...

TimeLike = datetime
