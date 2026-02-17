from __future__ import annotations

import pathlib
import sys
import unittest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
AGENTAPI_ROOT = PROJECT_ROOT / "libs" / "agentapi"
if str(AGENTAPI_ROOT) not in sys.path:
    sys.path.insert(0, str(AGENTAPI_ROOT))

from agents.financial_adviser.initial_prompt import _parse_json_object


class InitialPromptJsonParsingTests(unittest.TestCase):
    def test_allows_single_trailing_brace(self) -> None:
        payload = _parse_json_object('{"action":"query","queries":[{"key":"k","purpose":"p","sql":"SELECT 1"}]} }')
        self.assertEqual(payload["action"], "query")

    def test_strips_code_fences(self) -> None:
        payload = _parse_json_object("```json\n{\"action\":\"answer\",\"answer\":\"ok\"}\n```")
        self.assertEqual(payload["action"], "answer")

