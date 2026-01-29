from __future__ import annotations

from datetime import datetime
from typing import Protocol


class Fingerprintable(Protocol):
    """
    Request objects must expose a stable fingerprint for cache keys.

    Fingerprints should be deterministic for semantically identical requests and
    should not include non-essential ordering differences.
    """

    def fingerprint(self) -> str:
        ...


TimeLike = datetime
