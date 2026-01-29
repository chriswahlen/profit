"""
Core package initializer for the profit library.

We keep this minimal to avoid implicit side effects on import. Versioning is
centralized here so dependent modules can reference a single source of truth.
"""

__all__ = ["__version__"]

# Version is intentionally static until a release workflow is defined.
__version__ = "0.0.0"
