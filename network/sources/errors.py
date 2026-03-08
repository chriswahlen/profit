from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class ThrottledError(RuntimeError):
    """Raised when the provider asks us to back off."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class InactiveInstrumentError(RuntimeError):
    """Indicates the requested provider/instrument is not available."""

    def __init__(
        self,
        provider: str,
        provider_code: str,
        *,
        reason: str | None = None,
        requested_start: datetime | None = None,
        requested_end: datetime | None = None,
        active_from: datetime | None = None,
        active_to: datetime | None = None,
    ) -> None:
        super().__init__(
            f"inactive instrument {provider}:{provider_code} reason={reason} "
            f"requested=({requested_start},{requested_end}) active=({active_from},{active_to})"
        )
        self.provider = provider
        self.provider_code = provider_code
        self.reason = reason
        self.requested_start = requested_start
        self.requested_end = requested_end
        self.active_from = active_from
        self.active_to = active_to
