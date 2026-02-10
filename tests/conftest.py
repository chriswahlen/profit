from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AGENTAPI = ROOT / "libs" / "agentapi"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(AGENTAPI) not in sys.path:
    sys.path.insert(0, str(AGENTAPI))
