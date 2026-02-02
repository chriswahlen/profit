from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass
class RetrieverResult:
    payload: Mapping[str, Any]
    data_needs: list[Any] = field(default_factory=list)
    snippet_summaries: list[Any] = field(default_factory=list)


class BaseRetriever:
    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        raise NotImplementedError
