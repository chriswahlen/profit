class ThrottledError(RuntimeError):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class InactiveInstrumentError(RuntimeError):
    """
    Raised when a request targets an instrument outside its active lifecycle or missing from catalog.
    """

    def __init__(
        self,
        provider: str,
        provider_code: str,
        *,
        reason: str,
        requested_start,
        requested_end,
        active_from=None,
        active_to=None,
    ) -> None:
        self.provider = provider
        self.provider_code = provider_code
        self.reason = reason
        self.requested_start = requested_start
        self.requested_end = requested_end
        self.active_from = active_from
        self.active_to = active_to
        msg = (
            f"{provider}:{provider_code} inactive ({reason}); "
            f"requested {requested_start} → {requested_end}, "
            f"lifecycle {active_from} → {active_to or 'open'}"
        )
        super().__init__(msg)
