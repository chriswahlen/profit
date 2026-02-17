from __future__ import annotations

import pathlib
import unittest


class StartAgentApiScriptsTests(unittest.TestCase):
    def test_start_queue_script_invokes_worker(self) -> None:
        path = pathlib.Path("start-queue.sh")
        content = path.read_text(encoding="utf-8")
        self.assertIn("python3 -m service.main", content)
        self.assertIn("--db", content)
        self.assertIn('Config().data_path()', content)
        self.assertIn('agentapi.sqlite', content)
        self.assertIn("agents.financial_adviser.job_registry", content)
        self.assertIn("PYTHONPATH", content)

    def test_start_frontend_script_invokes_server(self) -> None:
        path = pathlib.Path("start-frontend.sh")
        content = path.read_text(encoding="utf-8")
        self.assertIn("python3 -m service.frontend.server", content)
        self.assertIn("--db", content)
        self.assertIn('Config().data_path()', content)
        self.assertIn('agentapi.sqlite', content)
        self.assertIn("127.0.0.1", content)
        self.assertIn("--default-job-type", content)
        self.assertIn("financial_adviser", content)
