from __future__ import annotations

import os
from pathlib import Path
from typing import MutableMapping

_CONFIG_LOADED = False
_DATA_ROOT_ENV_KEYS = ("PROFIT_DATA_ROOT", "PROFIT_DATA_DIR", "PROFIT_DATA")


def load_profit_conf(
    path: Path | str | None = None,
    *,
    env: MutableMapping[str, str] = os.environ,
) -> dict[str, str]:
    """
    Load key=value pairs from ~/.profit.conf (or provided path) into the given
    environment mapping when the key is not already present.

    Blank lines and lines starting with '#' are ignored.
    Returns a dict of parsed key/values.
    """
    conf_path = Path(path) if path is not None else Path.home() / ".profit.conf"
    if not conf_path.exists():
        return {}

    parsed: dict[str, str] = {}
    for line in conf_path.read_text().splitlines():
        striped = line.strip()
        if not striped or striped.startswith("#"):
            continue
        if "=" not in striped:
            continue
        key, val = striped.split("=", 1)
        key = key.strip()
        val = val.strip()
        parsed[key] = val
        env.setdefault(key, val)
    return parsed


def ensure_profit_conf_loaded() -> None:
    """
    Idempotently load ~/.profit.conf into os.environ (without overriding existing keys).
    """
    global _CONFIG_LOADED
    if _CONFIG_LOADED:
        return
    load_profit_conf()
    _CONFIG_LOADED = True


def get_data_root() -> Path:
    """
    Return the configured data root directory (defaults to ./data).
    """
    ensure_profit_conf_loaded()
    for key in _DATA_ROOT_ENV_KEYS:
        val = os.environ.get(key)
        if val:
            return Path(val)
    return Path("data")


def _reset_for_tests() -> None:  # pragma: no cover - test helper
    global _CONFIG_LOADED
    _CONFIG_LOADED = False
