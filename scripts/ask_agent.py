#!/usr/bin/env python
from __future__ import annotations

import argparse
import sys
import logging
from datetime import date
from pathlib import Path

from profit.agent.llm import ChatGPTLLM, StubLLM
from profit.agent.router import Router
from profit.agent.types import Question
from profit.agent import retrievers
from profit.cache.columnar_store import ColumnarSqliteStore


def _parse_date(val: str | None) -> date | None:
    return date.fromisoformat(val) if val else None


def main(argv: list[str] | None = None) -> int:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Ask a local data-aware agent (stub).")
    parser.add_argument("question", help="Natural language question.")
    parser.add_argument("--start", help="ISO start date (YYYY-MM-DD)", default=None)
    parser.add_argument("--end", help="ISO end date (YYYY-MM-DD)", default=None)
    parser.add_argument("--model", help="LLM model name", default="gpt-4.1-mini")
    parser.add_argument("--live", action="store_true", help="Use ChatGPTLLM instead of StubLLM (requires OPENAI_API_KEY).")
    parser.add_argument("--store-path", type=Path, help="Path to profit.sqlite (catalog/entity/redfin).")
    parser.add_argument("--columnar-path", type=Path, help="Path to columnar.sqlite3 for price data.")
    parser.add_argument("--edgar-docs", type=Path, help="Path to EDGAR docs directory.")
    args = parser.parse_args(argv)

    question = Question(text=args.question, start=_parse_date(args.start), end=_parse_date(args.end))

    router = Router()
    plan = router.route(question)
    if args.columnar_path:
        logging.info("prices: using columnar store path=%s", args.columnar_path)
    col_store = ColumnarSqliteStore(args.columnar_path) if args.columnar_path else None

    data = retrievers.fetch(
        plan,
        columnar_store=col_store,
        catalog_db_path=args.store_path,
        redfin_db_path=args.store_path,
        edgar_docs_path=args.edgar_docs,
        entity_store_path=args.store_path,
    )
    llm = ChatGPTLLM(model=args.model) if args.live else StubLLM(model=args.model)
    answer = llm.generate(question=question, plan=plan, data=data)
    print(answer.text)

    # Debug hint for unresolved inputs
    if isinstance(data.payload, dict):
        unresolved = []
        for key in ("unresolved", "unresolved_regions", "unresolved_filings"):
            vals = data.payload.get(key)
            if vals:
                unresolved.extend(vals if isinstance(vals, (list, tuple)) else [vals])
        if unresolved:
            print(f"[unresolved inputs] {', '.join(unresolved)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
