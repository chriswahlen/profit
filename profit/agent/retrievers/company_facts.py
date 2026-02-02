from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from profit.agent.retrievers.base import BaseRetriever, RetrieverResult
from profit.catalog.entity_store import EntityStore
from profit.config import get_columnar_db_path

logger = logging.getLogger(__name__)


def _resolve_entity_path(default: Path) -> Path:
    try:
        return get_columnar_db_path()
    except RuntimeError:
        return default


class CompanyFactsRetriever(BaseRetriever):
    def __init__(self, store: EntityStore | None = None, *, db_path: Path | None = None) -> None:
        if store:
            self.store = store
        else:
            base = db_path or _resolve_entity_path(Path("data/profit.sqlite"))
            self.store = EntityStore(base, readonly=True)

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("company_facts retriever fetching %s", request)
        results: list[dict] = []
        data_needs: list[dict] = []
        filings = request.get("filings") or []

        for company in request.get("companies") or []:
            entity_id = self._resolve_entity_id(company)
            if not entity_id:
                data_needs.append(
                    {
                        "name": company,
                        "reason": "entity not found",
                        "criticality": "high",
                        "error_code": "entity_not_found",
                        "company": company,
                    }
                )
                continue

            facts = []
            for field in request.get("fields") or []:
                key = field.get("key")
                if key is None:
                    continue
                fact_rows = self._query_facts(entity_id, key, filings)
                if not fact_rows:
                    continue
                facts.append({"field": key, "facts": fact_rows})

            if not facts:
                data_needs.append(
                    {
                        "name": company,
                        "reason": "no finance facts for requested fields",
                        "criticality": "medium",
                        "error_code": "fields_missing",
                        "company": company,
                        "fields": [field.get("key") for field in request.get("fields") or [] if field.get("key")],
                    }
                )
                continue

            results.append(
                {
                    "company": company,
                    "entity_id": entity_id,
                    "facts": facts,
                }
            )

        payload = {
            "type": "company_facts",
            "request": request,
            "data": results,
            "notes": notes,
        }
        return RetrieverResult(payload=payload, data_needs=data_needs)

    def _resolve_entity_id(self, company: str) -> str | None:
        return self.store.resolve_entity_id(company)

    def _query_facts(self, entity_id: str, key: str, filings: list[str]) -> list[dict]:
        return self.store.list_finance_facts(entity_id, key, filings=filings or None)
