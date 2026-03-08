from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Sequence

from data_sources.edgar.common import normalize_cik
from data_sources.edgar.edgar_data_store import EdgarDataStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConceptSeed:
    friendly_name: str
    description: str
    qnames: Sequence[str]


@dataclass(frozen=True)
class ConceptRegistrySnapshot:
    cic: str
    concept: str
    qnames: tuple[str, ...]


class ConceptRegistry:
    def __init__(
        self,
        per_cik: dict[str, dict[str, tuple[str, ...]]],
        seeds: Sequence[ConceptSeed],
    ) -> None:
        self._per_cik = per_cik
        self._seed_map = {seed.friendly_name: tuple(seed.qnames) for seed in seeds}

    def available_concepts(self, cik: str | None = None) -> Sequence[str]:
        if cik is None:
            return sorted(self._seed_map.keys())
        normalized = normalize_cik(cik)
        return sorted(self._per_cik.get(normalized, {}).keys())

    def qnames_for(self, cik: str, concept: str) -> Sequence[str]:
        normalized = normalize_cik(cik)
        concept_map = self._per_cik.get(normalized, {})
        if concept in concept_map:
            return concept_map[concept]
        if concept in self._seed_map:
            return self._seed_map[concept]
        raise KeyError(f"Unknown concept {concept}")

    def snapshot(self) -> Iterable[ConceptRegistrySnapshot]:
        for cik, concept_map in self._per_cik.items():
            for concept, qnames in concept_map.items():
                yield ConceptRegistrySnapshot(cic=cik, concept=concept, qnames=qnames)

    def serialize(self) -> dict[str, dict[str, list[str]]]:
        return {
            cik: {concept: list(qnames) for concept, qnames in concept_map.items()}
            for cik, concept_map in self._per_cik.items()
        }


class ConceptRegistryBuilder:
    """
    Builds a friendly-name → qname registry per CIK by scanning ingested EDGAR facts.
    """

    _QUERY = """
        SELECT
            a.cik,
            c.qname
        FROM xbrl_fact f
        JOIN edgar_accession a ON a.accession = f.accession
        JOIN xbrl_concept c ON c.concept_id = f.concept_id
        ORDER BY a.cik;
    """

    def __init__(self, store: EdgarDataStore, seeds: Sequence[ConceptSeed]) -> None:
        self._store = store
        self._seeds = seeds
        self._qname_to_concept = {
            qname: seed.friendly_name
            for seed in seeds
            for qname in seed.qnames
        }

    def build(self) -> ConceptRegistry:
        conn = self._store.connection
        cur = conn.cursor()
        cur.execute(self._QUERY)
        rows = cur.fetchall()
        per_cik: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for raw_cik, qname in rows:
            try:
                normalized = normalize_cik(raw_cik)
            except ValueError:
                continue
            concept = self._qname_to_concept.get(qname)
            if not concept:
                continue
            per_cik[normalized][concept].add(qname)
        normalized_map = {
            cik: {concept: tuple(sorted(qnames)) for concept, qnames in concept_map.items()}
            for cik, concept_map in per_cik.items()
        }
        logger.info("Concept registry built for %d companies", len(normalized_map))
        return ConceptRegistry(normalized_map, self._seeds)


CONCEPT_SEEDS: Sequence[ConceptSeed] = (
    ConceptSeed(
        friendly_name="assets",
        description="Total assets (instant or duration).",
        qnames=("us-gaap:Assets", "dei:EntityAssets"),
    ),
    ConceptSeed(
        friendly_name="liabilities",
        description="Total liabilities.",
        qnames=("us-gaap:Liabilities", "dei:EntityLiabilities"),
    ),
    ConceptSeed(
        friendly_name="revenue",
        description="Total revenue or sales.",
        qnames=("us-gaap:Revenues", "us-gaap:SalesRevenueNet", "dei:EntityRevenue"),
    ),
    ConceptSeed(
        friendly_name="net_income",
        description="Net income or loss (duration).",
        qnames=("us-gaap:NetIncomeLoss", "dei:EntityNetIncomeLoss"),
    ),
)
