"""
Lightweight, file-backed caching utilities used by fetchers.

The cache is intentionally simple and synchronous to keep it deterministic and
easy to test. It can be swapped out later with a different backend by
implementing the same interface.
"""

from .file_cache import CacheMissError, FileCache, OfflineModeError

__all__ = ["CacheMissError", "FileCache", "OfflineModeError"]
