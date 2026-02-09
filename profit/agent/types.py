from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class InsightSummary:
    insight_id: str
    title: str
    body: List[str]
    created_at: str = ""
    matched_tags: List[str] | None = None

