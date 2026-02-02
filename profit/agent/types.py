from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable


@dataclass
class Question:
    text: str
    start: date | None = None
    end: date | None = None
    hints: list[str] | None = None


@dataclass
class SnippetSummary:
    snippet_id: str
    title: str
    body: list[str]
    created_at: str
    matched_tags: list[str] | None = None

    def format(self) -> str:
        lines: list[str] = [
            f"{self.snippet_id}: {self.title} ({self.created_at})",
        ]
        if self.matched_tags:
            lines.append(f"tags: {', '.join(self.matched_tags)}")
        for line in self.body:
            lines.append(f"  • {line}")
        return "\n".join(lines)


@dataclass
class DataNeed:
    name: str
    provider: str | None = None
    reason: str | None = None
    criticality: str = "medium"

    def format(self) -> str:
        parts = [self.name, f"criticality={self.criticality}"]
        if self.provider:
            parts.append(f"provider={self.provider}")
        if self.reason:
            parts.append(f"reason={self.reason}")
        return " | ".join(parts)
