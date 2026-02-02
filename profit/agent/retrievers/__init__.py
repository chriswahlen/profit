from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Mapping

from profit.agent.snippets import SnippetStore
from profit.agent.types import SnippetSummary

logger = logging.getLogger(__name__)


@dataclass
class RetrieverResult:
    payload: Mapping[str, Any]
    data_needs: list[DataNeed] = field(default_factory=list)
    snippet_summaries: list[SnippetSummary] = field(default_factory=list)


class BaseRetriever:
    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        raise NotImplementedError


class MarketRetriever(BaseRetriever):
    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("market retriever stub received %s", request)
        data = {
            "type": "market",
            "request": request,
            "data": [],
            "notes": notes,
        }
        return RetrieverResult(payload=data)


class RealEstateRetriever(BaseRetriever):
    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("real_estate retriever stub received %s", request)
        data = {
            "type": "real_estate",
            "request": request,
            "data": [],
            "notes": notes,
        }
        return RetrieverResult(payload=data)


class CompanyFactsRetriever(BaseRetriever):
    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("company_facts retriever stub received %s", request)
        data = {
            "type": "company_facts",
            "request": request,
            "data": [],
            "notes": notes,
        }
        return RetrieverResult(payload=data)


class SnippetRetriever(BaseRetriever):
    def __init__(self, store: SnippetStore | None = None) -> None:
        self.store = store or SnippetStore()

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        action = request.get("action")
        if action == "store":
            snippet = request["snippet"]
            stored = self.store.store_snippet(snippet)
            return RetrieverResult(payload={"type": "snippet_store", "snippet": stored})

        if action == "lookup":
            filters = request.get("filters", {})
            matches = self.store.lookup(
                tags=filters.get("tags"),
                related_instruments=filters.get("related_instruments"),
                active_at=filters.get("active_at"),
                limit=request.get("limit", 5),
            )
            summaries = [
                SnippetSummary(
                    snippet_id=entry["snippet_id"],
                    title=entry["title"],
                    body=entry["body"],
                    created_at=entry["created_at"],
                    matched_tags=list(filters.get("tags") or []),
                )
                for entry in matches
            ]
            return RetrieverResult(
                payload={"type": "snippet_lookup", "matches": matches},
                snippet_summaries=summaries,
            )

        raise ValueError(f"unknown snippet action: {action}")


class RetrieverRegistry:
    def __init__(self, *, snippet_store: SnippetStore | None = None) -> None:
        self.snippet_store = snippet_store or SnippetStore()
        self._registry: dict[str, BaseRetriever] = {
            "market": MarketRetriever(),
            "real_estate": RealEstateRetriever(),
            "company_facts": CompanyFactsRetriever(),
            "snippet": SnippetRetriever(store=self.snippet_store),
        }

    def get(self, key: str) -> BaseRetriever:
        retriever = self._registry.get(key)
        if retriever is None:
            raise KeyError(f"no retriever registered for {key}")
        return retriever
