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
        cur = self.store.conn.cursor()
        cur.execute(
            "SELECT entity_id FROM entity WHERE entity_id = ?",
            (company,),
        )
        row = cur.fetchone()
        if row:
            return row["entity_id"]
        cur.execute(
            "SELECT entity_id FROM entity_identifier WHERE value = ? COLLATE NOCASE",
            (company,),
        )
        identifier = cur.fetchone()
        if identifier:
            return identifier["entity_id"]
        curated = company.upper()
        cur.execute(
            "SELECT entity_id FROM entity_identifier WHERE value = ? COLLATE NOCASE",
            (curated,),
        )
        identifier = cur.fetchone()
        if identifier:
            return identifier["entity_id"]
        return None

    def _query_facts(self, entity_id: str, key: str, filings: list[str]) -> list[dict]:
        if not filings:
            filings = ["%"]
        like_clauses = " OR ".join("report_id LIKE ?" for _ in filings)
        sql = f"""
            SELECT report_id, report_key, period_start, period_end, filed_at, units, value, asof
            FROM company_finance_fact
            WHERE entity_id = ?
              AND report_key = ?
              AND ({like_clauses})
            ORDER BY period_end DESC
            LIMIT 5
        """
        params = [entity_id, key] + [f"{filing}%" for filing in filings]
        cur = self.store.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [
            {
                "report_id": row["report_id"],
                "report_key": row["report_key"],
                "period_start": row["period_start"],
                "period_end": row["period_end"],
                "filed_at": row["filed_at"],
                "units": row["units"],
                "value": row["value"],
                "asof": row["asof"],
            }
            for row in rows
        ]
