from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProfitConfig:
    data_root: Path
    cache_root: Path
    store_path: Path
    log_level: str
    refresh_catalog: bool
