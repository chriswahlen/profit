from __future__ import annotations

import pathlib
import sys
import json
import unittest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
AGENTAPI_ROOT = PROJECT_ROOT / "libs" / "agentapi"
if str(AGENTAPI_ROOT) not in sys.path:
    sys.path.insert(0, str(AGENTAPI_ROOT))

from agentapi.components.memory_snapshot_store import MemorySnapshotStore
from llm.stub_llm import StubLLM

from agents.financial_adviser.state_machine import build_financial_adviser_state_machine


class FinancialAdviserStateMachineBasicTests(unittest.TestCase):
    def test_single_stage_question_answer_completes(self) -> None:
        execution_id = "exec_test_financial_adviser_basic"
        question = "Should I pay off high-interest debt or invest?"
        expected_answer = "Paying off high-interest debt usually has a strong guaranteed return."

        store = MemorySnapshotStore()
        backend = StubLLM(key_responses={"User question": expected_answer})

        machine, returned_store = build_financial_adviser_state_machine(
            execution_id=execution_id,
            question=question,
            snapshot_store=store,
            llm_backend=backend,
        )
        self.assertIs(returned_store, store)

        # Execute until terminal.
        for _ in range(5):
            polled = machine.poll_ready()
            if polled.is_done:
                break
            attempted = machine.execute_ready_batch()
            self.assertTrue(attempted)

        polled = machine.poll_ready()
        self.assertTrue(polled.is_done)
        self.assertEqual(polled.terminal_reason, "completed")

        self.assertIn("financial_adviser", machine.user_context)
        self.assertEqual(machine.user_context["financial_adviser"]["question"], question)
        self.assertEqual(machine.user_context["financial_adviser"]["answer"], expected_answer)
        self.assertEqual(machine.user_context["financial_adviser"]["status"], "completed")

        snap = store.load_snapshot(execution_id)
        assert snap is not None
        ctx = json.loads(snap.user_context_json)
        self.assertEqual(ctx["financial_adviser"]["answer"], expected_answer)

        history = store.load_history(execution_id, after_cursor=0)
        self.assertEqual(len(history), 1)
        _cursor, entry = history[0]
        self.assertEqual(entry.status, "succeeded")
        self.assertEqual(entry.stage_name, "financial_adviser.qa")
