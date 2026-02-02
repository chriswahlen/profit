from __future__ import annotations

from profit.agent.retrievers.company_facts import CompanyFactsRetriever
from profit.agent.retrievers.market import MarketRetriever
from profit.agent.retrievers.real_estate import RealEstateRetriever
from profit.agent.retrievers.snippet import SnippetRetriever
from profit.agent.snippets import SnippetStore
from profit.agent.retrievers.base import BaseRetriever


class RetrieverRegistry:
    def __init__(self, *, snippet_store: SnippetStore | None = None) -> None:
        store = snippet_store or SnippetStore()
        self._registry: dict[str, BaseRetriever] = {
            "market": MarketRetriever(),
            "real_estate": RealEstateRetriever(),
            "company_facts": CompanyFactsRetriever(),
            "snippet": SnippetRetriever(store=store),
        }

    def get(self, key: str) -> BaseRetriever:
        retriever = self._registry.get(key)
        if retriever is None:
            raise KeyError(f"no retriever registered for {key}")
        return retriever
