from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


class CacheMissError(Exception):
    """Raised when the requested key is not available or is expired."""


class OfflineModeError(Exception):
    """Raised when an operation requires network access while offline mode is enabled."""


@dataclass(frozen=True)
class CacheEntry:
    """Simple holder that mirrors the interface expected by BaseFetcher."""

    value: Any


class FileCache:
    """File-backed cache used by BaseFetcher."""

    def __init__(self, base_dir: Path | str | None = None, ttl: timedelta = timedelta(days=30)) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else None
        self.ttl = ttl
        self._store: Dict[str, Tuple[Any, float]] = {}

    def _path_for_key(self, key: str) -> Path:
        if self.base_dir is None:
            raise RuntimeError("File cache path requested without base_dir")
        hashed = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.base_dir / f"{hashed}.cache"

    def get(self, key: str, *, ttl: timedelta | None = None) -> CacheEntry:
        ttl = ttl or self.ttl
        if self.base_dir:
            path = self._path_for_key(key)
            if not path.exists():
                raise CacheMissError(key)
            with path.open("rb") as fh:
                ts_line = fh.readline()
                try:
                    timestamp = float(ts_line.decode("ascii").strip())
                except Exception:
                    path.unlink(missing_ok=True)
                    raise CacheMissError(key)
                data = fh.read()
            age = time.time() - timestamp
            if ttl and age > ttl.total_seconds():
                path.unlink(missing_ok=True)
                raise CacheMissError(key)
            return CacheEntry(value=data)
        entry = self._store.get(key)
        if entry is None:
            raise CacheMissError(key)
        value, timestamp = entry
        age = time.time() - timestamp
        if ttl and age > ttl.total_seconds():
            del self._store[key]
            raise CacheMissError(key)
        return CacheEntry(value=value)

    def set(self, key: str, value: Any) -> None:
        if self.base_dir:
            path = self._path_for_key(key)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("wb") as fh:
                fh.write(f"{time.time()}\n".encode("ascii"))
                fh.write(value)
            return
        self._store[key] = (value, time.time())
