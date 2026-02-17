from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import unittest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
AGENTAPI_ROOT = PROJECT_ROOT / "libs" / "agentapi"
if str(AGENTAPI_ROOT) not in sys.path:
    sys.path.insert(0, str(AGENTAPI_ROOT))

from config import Config

from agents.financial_adviser.job_registry import _resolve_openai_api_key


class OpenAiKeyResolutionTests(unittest.TestCase):
    def test_reads_from_profit_config_when_env_missing(self) -> None:
        old_env = os.environ.pop("OPENAI_API_KEY", None)
        old_cfg = os.environ.get("PROFIT_CONFIG")
        try:
            with tempfile.TemporaryDirectory() as td:
                cfg_path = pathlib.Path(td) / "profit.conf"
                cfg_path.write_text("OPENAI_API_KEY=sk-test\n", encoding="utf-8")
                os.environ["PROFIT_CONFIG"] = str(cfg_path)
                cfg = Config()
                self.assertEqual(_resolve_openai_api_key(cfg), "sk-test")
        finally:
            if old_env is not None:
                os.environ["OPENAI_API_KEY"] = old_env
            if old_cfg is None:
                os.environ.pop("PROFIT_CONFIG", None)
            else:
                os.environ["PROFIT_CONFIG"] = old_cfg

