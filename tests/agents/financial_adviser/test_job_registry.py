from __future__ import annotations

import pathlib
import sys
import json
import os
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
AGENTAPI_ROOT = PROJECT_ROOT / "libs" / "agentapi"
if str(AGENTAPI_ROOT) not in sys.path:
    sys.path.insert(0, str(AGENTAPI_ROOT))

from service.queue import JobQueue

from agents.financial_adviser.job_registry import JOB_TYPE_FINANCIAL_ADVISER, register_jobs


class FinancialAdviserJobRegistryTests(unittest.TestCase):
    def test_enqueue_and_run_job(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            old = os.environ.get("PROFIT_DATA_PATH")
            os.environ["PROFIT_DATA_PATH"] = td
            db_path = Path(td) / "agentapi.sqlite"
            queue = JobQueue(db_path=db_path)
            try:
                register_jobs(queue, live=False)
                job = queue.enqueue(job_type=JOB_TYPE_FINANCIAL_ADVISER, payload={"prompt": "What is an ETF?"})
                result = queue.run_next()
                assert result is not None
                self.assertEqual(result.job_id, job.job_id)
                self.assertEqual(result.status, "completed")

                # Verify snapshot user_context includes an answer.
                row = queue._conn.execute(  # pylint: disable=protected-access
                    "SELECT user_context_json FROM snapshot WHERE execution_id = ?",
                    (job.execution_id,),
                ).fetchone()
                assert row is not None
                ctx = json.loads(row[0])
                self.assertIn("financial_adviser", ctx)
                self.assertIn("answer", ctx["financial_adviser"])
            finally:
                queue.close()
                if old is None:
                    os.environ.pop("PROFIT_DATA_PATH", None)
                else:
                    os.environ["PROFIT_DATA_PATH"] = old
