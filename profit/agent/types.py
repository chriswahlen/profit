from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal, Sequence

SourceKind = Literal["prices", "redfin", "edgar", "unknown"]


@dataclass(frozen=True)
class Question:
    text: str
    start: date | None = None
    end: date | None = None
    provider_hint: str | None = None


@dataclass(frozen=True)
class RetrievalPlan:
    source: SourceKind
    instruments: Sequence[str] = ()
    regions: Sequence[str] = ()
    filings: Sequence[str] = ()
    start: date | None = None
    end: date | None = None
    notes: str | None = None


@dataclass(frozen=True)
class RetrievedData:
    source: SourceKind
    payload: Any = None
    start: date | None = None
    end: date | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Answer:
    text: str
    supporting: RetrievedData | None = None
