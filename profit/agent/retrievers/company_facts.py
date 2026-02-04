from __future__ import annotations

import logging
import shlex
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Protocol

from profit.agent.retrievers.base import BaseRetriever, RetrieverResult
from profit.catalog.entity_store import EntityStore
from profit.catalog.identifier_utils import resolve_entity_id_from_identifier
from profit.config import ProfitConfig, get_columnar_db_path

logger = logging.getLogger(__name__)


def _resolve_entity_path(default: Path) -> Path:
    try:
        return get_columnar_db_path()
    except RuntimeError:
        return default


class CatchupRunner(Protocol):
    def run(self, cik: str, cfg: ProfitConfig) -> None:
        ...


class CompanyFactsRetriever(BaseRetriever):
    def __init__(
        self,
        store: EntityStore | None = None,
        *,
        db_path: Path | None = None,
        catchup_runner: CatchupRunner | None = None,
        catchup_config: ProfitConfig | None = None,
    ) -> None:
        if store:
            self.store = store
        else:
            base = db_path or _resolve_entity_path(Path("data/profit.sqlite"))
            self.store = EntityStore(base, readonly=True)
        self._catchup_runner = catchup_runner or _DefaultEdgarCatchupRunner()
        self._catchup_cfg = catchup_config
        self._catchup_done: set[str] = set()

    def fetch(self, request: dict, *, notes: str | None = None) -> RetrieverResult:
        logger.info("company_facts retriever fetching %s", request)
        results: list[dict] = []
        data_needs: list[dict] = []
        filings = request.get("filings") or []
        fields = request.get("fields") or []
        company_entities: list[tuple[str, str | None]] = []

        for company in request.get("companies") or []:
            entity_id = self._resolve_entity_id(company)
            company_entities.append((company, entity_id))
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

        data_needs.extend(self._run_edgar_catch_up(company_entities))

        for company, entity_id in company_entities:
            if not entity_id:
                continue

            facts: list[dict] = []
            for field in fields:
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
                        "fields": [field.get("key") for field in fields if field.get("key")],
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
        return resolve_entity_id_from_identifier(self.store, company)

    def _query_facts(self, entity_id: str, key: str, filings: list[str]) -> list[dict]:
        return self.store.list_finance_facts(entity_id, key, filings=filings or None)

    def _run_edgar_catch_up(self, company_entities: list[tuple[str, str | None]]) -> list[dict]:
        cik_to_companies: dict[str, list[str]] = defaultdict(list)
        for company, entity_id in company_entities:
            if not entity_id:
                continue
            cik = self._resolve_cik_for_entity(entity_id)
            if cik:
                cik_to_companies[cik].append(company)
        if not cik_to_companies:
            return []

        cfg = self._get_catchup_config()
        if cfg is None:
            logger.warning("edgar catch-up skipped; configuration unavailable")
            return []

        needs: list[dict] = []
        for cik, companies in cik_to_companies.items():
            if cik in self._catchup_done:
                continue
            try:
                self._catchup_runner.run(cik, cfg)
            except Exception as exc:  # pragma: no cover - best-effort logging
                logger.warning("edgar catch-up failed for cik=%s: %s", cik, exc)
                reason = str(exc)
                for company in companies:
                    needs.append(
                        {
                            "name": company,
                            "reason": f"edgar catch-up failed: {reason}",
                            "criticality": "high",
                            "error_code": "edgar_catchup_failed",
                            "company": company,
                        }
                    )
                continue
            self._catchup_done.add(cik)
        return needs

    def _resolve_cik_for_entity(self, entity_id: str) -> str | None:
        return self.store.resolve_identifier(entity_id, "sec:cik", provider_id="sec:edgar")

    def _get_catchup_config(self) -> ProfitConfig | None:
        if self._catchup_cfg is not None:
            return self._catchup_cfg
        cfg = self._resolve_catchup_config()
        if cfg:
            self._catchup_cfg = cfg
        return cfg

    def _resolve_catchup_config(self) -> ProfitConfig | None:
        try:
            data_root = ProfitConfig.resolve_data_root()
        except RuntimeError as exc:
            logger.warning("edgar catch-up skipped; data root missing: %s", exc)
            return None
        try:
            cache_root = ProfitConfig.resolve_cache_root()
        except RuntimeError:
            cache_root = data_root / "cache"
        try:
            store_path = ProfitConfig.resolve_columnar_db_path()
        except RuntimeError as exc:
            logger.warning("edgar catch-up skipped; store path unavailable: %s", exc)
            return None
        return ProfitConfig(
            data_root=data_root,
            cache_root=cache_root,
            store_path=store_path,
            log_level="INFO",
            refresh_catalog=False,
        )


class _DefaultEdgarCatchupRunner:
    def __init__(self, *, python: str | None = None, script_root: Path | None = None):
        self.python = python or sys.executable
        root = script_root or Path(__file__).resolve().parents[3]
        scripts = root / "scripts"
        self.fetch_script = scripts / "fetch_edgar.py"
        self.extract_script = scripts / "extract_edgar_facts.py"

    def run(self, cik: str, cfg: ProfitConfig) -> None:
        self._run_script(
            self.fetch_script,
            [
                cik,
                "--data-root",
                str(cfg.data_root),
                "--cache-dir",
                str(cfg.cache_root),
                "--store-path",
                str(cfg.store_path),
            ],
        )
        self._run_script(
            self.extract_script,
            [
                "--cik",
                cik,
                "--data-root",
                str(cfg.data_root),
                "--store-path",
                str(cfg.store_path),
                "--edgar-db",
                str(cfg.data_root / "edgar.sqlite3"),
            ],
        )

    def _run_script(self, script: Path, args: list[str]) -> None:
        if not script.exists():
            raise FileNotFoundError(f"missing edgar script: {script}")
        cmd = [self.python, str(script)] + args
        logger.info("edgar catch-up command: %s", " ".join(shlex.quote(part) for part in cmd))
        subprocess.run(cmd, check=True)
