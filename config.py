"""Minimal configuration helper.

The goal here is simply to give the data stores somewhere predictable to write
their SQLite files. The config is intentionally small until the broader app
needs more structure.
"""

from __future__ import annotations

import configparser
import os
from pathlib import Path
from typing import Optional


class Config:
    """Reads config from a file or environment with lightweight defaults."""

    def __init__(self, path: str | None = None):
        self.path = Path(path or os.getenv("PROFIT_CONFIG", "~/.profit.conf")).expanduser()
        self._parser = configparser.ConfigParser()
        if self.path.exists():
            try:
                self._parser.read(self.path)
            except configparser.MissingSectionHeaderError:
                # Support simple KEY=VALUE files by wrapping them in a DEFAULT section.
                content = self.path.read_text()
                wrapped = "[DEFAULT]\n" + content
                self._parser.read_string(wrapped)

        # Cache the resolved data path so we do not resolve it repeatedly.
        self._data_path: Optional[Path] = None

    def data_path(self) -> str:
        """Return a filesystem location to store data files.

        Priority order:
        1) PROFIT_DATA_PATH env var
        2) "paths" section in the config file with key "data_path"
        3) ./.profit/data (repo-local, sandbox-friendly)
        4) ~/.profit/data (fallback for environments without sandbox limits)
        The directory is created if it does not exist.
        """

        if self._data_path is not None:
            return str(self._data_path)

        env_path = os.getenv("PROFIT_DATA_PATH")
        cfg_path = None
        if self._parser.has_section("paths"):
            cfg_path = self._parser.get("paths", "data_path", fallback=None)

        candidates = [
            env_path,
            cfg_path,
            str(Path.cwd() / ".profit" / "data"),
            "~/.profit/data",
        ]

        first_error: Exception | None = None
        for raw_path in candidates:
            if not raw_path:
                continue
            try:
                p = Path(raw_path).expanduser()
                p.mkdir(parents=True, exist_ok=True)
                self._data_path = p
                return str(self._data_path)
            except Exception as exc:  # noqa: BLE001 - capture for fallback logging
                if first_error is None:
                    first_error = exc
                continue

        # If we exhausted all options, raise the first error for debugging.
        raise first_error or RuntimeError("Unable to determine data path")

    def get_key(self, key_name: str) -> Optional[str]:
        """Fetch a config value from env, falling back to the file."""

        env_key = os.getenv(key_name)
        if env_key is not None:
            return env_key

        if self._parser.has_option("DEFAULT", key_name):
            return self._parser.get("DEFAULT", key_name)

        for section in self._parser.sections():
            if self._parser.has_option(section, key_name):
                return self._parser.get(section, key_name)

        return None
