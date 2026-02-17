from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import unittest
from datetime import datetime, timezone

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
AGENTAPI_ROOT = PROJECT_ROOT / "libs" / "agentapi"
if str(AGENTAPI_ROOT) not in sys.path:
    sys.path.insert(0, str(AGENTAPI_ROOT))

from agentapi.components.memory_snapshot_store import MemorySnapshotStore
from llm.stub_llm import StubLLM

from agents.financial_adviser.state_machine import build_financial_adviser_state_machine
from config import Config
from data_sources.edgar.edgar_data_store import EdgarDataStore


class FinancialAdviserEdgarSqlLoopTests(unittest.TestCase):
    def test_agent_requests_sql_then_answers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            old = os.environ.get("PROFIT_DATA_PATH")
            os.environ["PROFIT_DATA_PATH"] = td
            try:
                cfg = Config()
                edgar = EdgarDataStore(cfg)
                # Seed one submissions row to query.
                edgar.upsert_submissions_rows(
                    [
                        (
                            "0000320193",
                            "Apple Inc.",
                            datetime.now(timezone.utc),
                            json.dumps({"name": "Apple Inc."}, ensure_ascii=True),
                        )
                    ]
                )

                backend = StubLLM(
                    key_responses={
                        "ROUND: 0": json.dumps(
                            {
                                "action": "query",
                                "plan": {
                                    "description": "Use EDGAR to confirm the company exists and retrieve identifying metadata.",
                                    "instructions": "Start with submissions lookup; then answer using retrieved CIK/entity_name.",
                                },
                                "goal": "Find evidence in our EDGAR DB that Apple exists and fetch its CIK.",
                                "queries": [
                                    {
                                        "key": "apple_lookup",
                                        "purpose": "Confirm Apple exists in edgar_submissions and fetch its CIK",
                                        "sql": "SELECT cik, entity_name FROM edgar_submissions WHERE entity_name LIKE '%Apple%' LIMIT 50;",
                                    }
                                ],
                            }
                        ),
                        "ROUND: 1": json.dumps(
                            {
                                "action": "answer",
                                "goal": "Use the query results to answer the user's question.",
                                "answer": "Apple is present in EDGAR submissions (stub answer).",
                            }
                        ),
                    }
                )

                store = MemorySnapshotStore()
                machine, _ = build_financial_adviser_state_machine(
                    execution_id="exec_test_edgar_sql_loop",
                    question="What is Apple's CIK and do we have it in our EDGAR DB?",
                    snapshot_store=store,
                    llm_backend=backend,
                    edgar_store=edgar,
                )

                # Drive until terminal.
                for _ in range(10):
                    polled = machine.poll_ready()
                    if polled.is_done:
                        break
                    machine.execute_ready_batch()

                polled = machine.poll_ready()
                self.assertTrue(polled.is_done)
                fa = machine.user_context["financial_adviser"]
                self.assertEqual(fa["status"], "completed")
                self.assertIn("Apple", fa["answer"])
                self.assertIsInstance(fa.get("db_results"), list)
                self.assertGreaterEqual(len(fa["db_results"]), 1)
                first = fa["db_results"][0]
                self.assertEqual(first["key"], "apple_lookup")
                self.assertIn("Apple exists", first["purpose"])
                self.assertEqual(first["columns"], ["cik", "entity_name"])
                self.assertEqual(first["rows"][0][0], "0000320193")
            finally:
                if old is None:
                    os.environ.pop("PROFIT_DATA_PATH", None)
                else:
                    os.environ["PROFIT_DATA_PATH"] = old
