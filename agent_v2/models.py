from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Optional


def _date_to_iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


def _date_from_iso(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    return date.fromisoformat(s)


@dataclass(frozen=True)
class Insight:
    text: str
    tags: tuple[str, ...]
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "tags": list(self.tags),
            "start_date": _date_to_iso(self.start_date),
            "end_date": _date_to_iso(self.end_date),
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "Insight":
        text = str(data.get("text", "")).strip()
        tags_raw = data.get("tags") or []
        tags = tuple(str(t).strip() for t in tags_raw if str(t).strip())
        return Insight(
            text=text,
            tags=tags,
            start_date=_date_from_iso(data.get("start_date")),
            end_date=_date_from_iso(data.get("end_date")),
        )


@dataclass(frozen=True)
class DataRequest:
    key: str
    request: str
    why: str

    def to_dict(self) -> dict[str, str]:
        return {"key": self.key, "request": self.request, "why": self.why}

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "DataRequest":
        return DataRequest(
            key=str(data.get("key", "")).strip(),
            request=str(data.get("request", "")).strip(),
            why=str(data.get("why", "")).strip(),
        )

