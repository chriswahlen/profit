"""Utility helpers shared across the profit package."""

from .url_fetcher import PermanentFetchError, TemporaryFetchError, fetch_url

__all__ = ["fetch_url", "PermanentFetchError", "TemporaryFetchError"]
