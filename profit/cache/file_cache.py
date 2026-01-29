from __future__ import annotations

import hashlib
import os
import pickle
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Generic, Optional, TypeVar

from profit.config import ensure_profit_conf_loaded

class CacheMissError(KeyError):
    """Raised when a cache lookup fails."""


class OfflineModeError(RuntimeError):
    """Raised when offline mode prevents a network fetch and cache is empty."""


T = TypeVar("T")


def _default_cache_dir() -> Path:
    ensure_profit_conf_loaded()
    base = os.environ.get("PROFIT_CACHE_DIR") or os.environ.get("PROFIT_CACHE_ROOT") or os.environ.get("PROFIT_CACHE")
    return Path(base) if base else Path(".cache") / "profit"


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class CacheEntry(Generic[T]):
    """Metadata about a cached value."""

    key: str
    path: Path
    created_at: datetime
    value: T


class FileCache(Generic[T]):
    """
    Simple file-backed cache with TTL support.

    Values are serialized with pickle for flexibility. Callers should only store
    data they trust because pickle is not safe for untrusted input.
    """

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        ttl: timedelta = timedelta(days=30),
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self.base_dir = base_dir or _default_cache_dir()
        self.ttl = ttl
        self._clock = clock
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        safe_prefix = re.sub(r"[^A-Za-z0-9_.-]", "_", key)[:32]
        filename = f"{safe_prefix}__{digest[:12]}.pkl"
        return self.base_dir / filename

    def _is_expired(self, path: Path, ttl: Optional[timedelta]) -> bool:
        ttl = ttl if ttl is not None else self.ttl
        if ttl is None:
            return False
        created_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return (self._clock() - created_at) > ttl

    def get(self, key: str, ttl: Optional[timedelta] = None) -> CacheEntry[T]:
        """
        Retrieve a cached entry. Raises CacheMissError on miss or expiry.
        """
        path = self._key_to_path(key)
        if not path.exists() or self._is_expired(path, ttl):
            raise CacheMissError(key)

        with path.open("rb") as fh:
            value = pickle.load(fh)

        created_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return CacheEntry(key=key, path=path, created_at=created_at, value=value)

    def set(self, key: str, value: T) -> CacheEntry[T]:
        """
        Persist a value in the cache, returning the resulting entry metadata.
        """
        path = self._key_to_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write atomically to avoid partial reads from concurrent processes.
        with tempfile.NamedTemporaryFile(delete=False, dir=path.parent) as tmp:
            pickle.dump(value, tmp)
            temp_path = Path(tmp.name)
        temp_path.replace(path)

        created_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return CacheEntry(key=key, path=path, created_at=created_at, value=value)

    def clear(self, key: str) -> None:
        """
        Remove a single cache entry if present. Missing keys are ignored.
        """
        path = self._key_to_path(key)
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def purge_all(self) -> None:
        """Remove all cached entries under the base directory."""
        if not self.base_dir.exists():
            return
        for path in self.base_dir.glob("*.pkl"):
            try:
                path.unlink()
            except FileNotFoundError:
                continue
