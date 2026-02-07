from __future__ import annotations

from profit.agent.retrievers.market import MarketRetriever
from profit.agent.retrievers.real_estate import RealEstateRetriever
from profit.agent.retrievers.insight import InsightRetriever
from profit.agent.insights import InsightStore
from profit.agent.retrievers.base import BaseRetriever


class RetrieverRegistry:
    def __init__(self, *, insight_store: InsightStore | None = None) -> None:
        store = insight_store or InsightStore()
        self._registry: dict[str, BaseRetriever] = {
            "market": MarketRetriever(),
            "real_estate": RealEstateRetriever(),
            "insight": InsightRetriever(store=store),
        }

    def get(self, key: str) -> BaseRetriever:
        retriever = self._registry.get(key)
        if retriever is None:
            raise KeyError(f"no retriever registered for {key}")
        return retriever
