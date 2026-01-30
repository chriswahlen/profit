from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


@dataclass(frozen=True)
class FundamentalsRequest:
    instrument_id: str
    provider: str  # always "sec"
    provider_code: str  # CIK10
    start: datetime  # filed/accepted window start (inclusive)
    end: datetime  # filed/accepted window end (inclusive)
    forms: tuple[str, ...] = ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A", "40-F", "40-F/A")

    def fingerprint(self) -> str:
        return "|".join(
            [
                "sec",
                self.provider_code,
                self.start.isoformat(),
                self.end.isoformat(),
                ",".join(self.forms),
            ]
        )


@dataclass(frozen=True)
class FilingRow:
    provider: str
    provider_code: str
    instrument_id: str
    accession: str
    form: str
    filed_at: datetime
    accepted_at: datetime | None
    known_at: datetime
    report_period_end: datetime | None
    is_amendment: bool
    asof: datetime
    attrs: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class FactRow:
    instrument_id: str
    provider: str
    provider_code: str
    accession: str
    form: str
    filed_at: datetime
    accepted_at: datetime | None
    known_at: datetime
    asof: datetime
    tag_qname: str
    period_start: datetime | None
    period_end: datetime
    unit: str
    currency: str | None
    dims_json: str
    dims_key: str
    dims_hash: str
    value_kind: str  # "number" | "text"
    value_num: float | None = None
    value_text: str | None = None
    value_text_preview: str | None = None
    value_text_len: int | None = None
    value_text_truncated: bool | None = None
    statement: str | None = None  # "is" | "bs" | "cf" | None
    line_item_code: str | None = None
    decimals: int | None = None
    attrs: Mapping[str, Any] | None = None
