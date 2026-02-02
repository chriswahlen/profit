from __future__ import annotations

import re
from datetime import date
from typing import Sequence

from profit.agent.types import Question, RetrievalPlan, SourceKind


_PRICE_HINTS = ("price", "prices", "ohlcv", "bar", "return", "volatility")
_REDFIN_HINTS = ("inventory", "median", "housing", "redfin", "dom", "sale-to-list")
_EDGAR_HINTS = ("10-k", "10q", "10-k", "filing", "edgar", "sec", "footnote")


def _normalize_dates(start: date | None, end: date | None) -> tuple[date | None, date | None]:
    if start and end and start > end:
        return end, start
    return start, end


def _has_any(text: str, needles: Sequence[str]) -> bool:
    lower = text.lower()
    return any(n in lower for n in needles)


class Router:
    """
    Lightweight heuristic router to pick a primary data source for a question.
    Avoids network calls; can be swapped for a learned router later.
    """

    def route(self, question: Question) -> RetrievalPlan:
        start, end = _normalize_dates(question.start, question.end)
        source = self._classify(question.text)
        instruments = self._extract_tickers(question.text) if source == "prices" else ()
        regions: tuple[str, ...] = ()
        filings: tuple[str, ...] = ()
        notes = None
        if source == "redfin":
            regions = self._extract_regions_hint(question.text)
        if source == "edgar":
            filings = self._extract_filings_hint(question.text)
        return RetrievalPlan(
            source=source,
            instruments=instruments,
            regions=regions,
            filings=filings,
            start=start,
            end=end,
            notes=notes,
        )

    def _classify(self, text: str) -> SourceKind:
        lower = text.lower()
        if _has_any(lower, _PRICE_HINTS):
            return "prices"
        if _has_any(lower, _REDFIN_HINTS):
            return "redfin"
        if _has_any(lower, _EDGAR_HINTS):
            return "edgar"
        # Fallback: if a ticker pattern is present, assume prices.
        if self._extract_tickers(text):
            return "prices"
        return "unknown"

    def _extract_tickers(self, text: str) -> tuple[str, ...]:
        # Naive ticker capture: 1-5 uppercase letters, not at word start of digits.
        candidates = set(re.findall(r"\b[A-Z]{1,5}\b", text))
        return tuple(sorted(candidates))

    def _extract_regions_hint(self, text: str) -> tuple[str, ...]:
        # Placeholder: in future map to region ids via CatalogStore.
        return ()

    def _extract_filings_hint(self, text: str) -> tuple[str, ...]:
        # Placeholder: extract CIK or accession if present.
        cik_like = re.findall(r"\b\d{10}\b", text)
        return tuple(sorted(set(cik_like)))
