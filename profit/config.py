from __future__ import annotations

import os
from pathlib import Path
from typing import MutableMapping, Iterable

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
    Return the configured data root directory (requires explicit config).
    """
    ensure_profit_conf_loaded()
    for key in _DATA_ROOT_ENV_KEYS:
        val = os.environ.get(key)
        if val:
            return Path(val)
    raise RuntimeError("Data root not configured; set PROFIT_DATA_ROOT or use --data-root equivalent")


def get_columnar_db_path(*, args=None, filename: str = "columnar.sqlite3") -> Path:
    if args is not None and getattr(args, "store_path", None):
        return Path(args.store_path)
    return get_data_root() / filename


def get_catalog_db_path(*, args=None, filename: str = "catalog.sqlite3") -> Path:
    """
    Resolve catalog path:
    - If CLI args provides catalog_path, use it.
    - Else env/ ~/.profit.conf via PROFIT_CATALOG_PATH/PROFIT_CATALOG/PROFIT_CATALOG_DB.
    - Else default to PROFIT_DATA_ROOT/<filename>.
    """
    if args is not None and getattr(args, "catalog_path", None):
        return Path(args.catalog_path)
    env_val = get_setting("PROFIT_CATALOG_PATH", "PROFIT_CATALOG", "PROFIT_CATALOG_DB")
    if env_val:
        return Path(env_val)
    return get_data_root() / filename


def _reset_for_tests() -> None:  # pragma: no cover - test helper
    global _CONFIG_LOADED
    _CONFIG_LOADED = False


def get_setting(*keys: str, default: str | None = None) -> str | None:
    """
    Return the first non-empty env value among keys (after loading ~/.profit.conf).
    """
    ensure_profit_conf_loaded()
    for key in keys:
        val = os.environ.get(key)
        if val:
            return val
    return default


def get_path_setting(*keys: str, default: str | Path) -> Path:
    val = get_setting(*keys)
    if val:
        return Path(val)
    return Path(default)


def get_cache_root(*, args=None) -> Path:
    """
    Resolve cache root:
    - If CLI args provides cache_dir, use it.
    - Else env/ ~/.profit.conf via PROFIT_CACHE_DIR/PROFIT_CACHE_ROOT.
    - Else raise.
    """
    if args is not None and getattr(args, "cache_dir", None):
        return Path(args.cache_dir)
    val = get_setting("PROFIT_CACHE_DIR", "PROFIT_CACHE_ROOT", "PROFIT_CACHE")
    if not val:
        raise RuntimeError("Cache root not configured; set PROFIT_CACHE_DIR or PROFIT_CACHE_ROOT or provide --cache-dir")
    return Path(val)


def add_common_cli_args(
    parser,
    *,
    cache_help_subdir: str = "fetcher",
    default_store_filename: str = "columnar.sqlite3",
    include_catalog_path: bool = False,
    default_catalog_filename: str = "catalog.sqlite3",
):
    """
    Add shared CLI arguments for cache/store/log level.

    - --cache-dir: overrides PROFIT_CACHE_* roots.
    - --store-path: overrides PROFIT_DATA_ROOT/<default_store_filename>.
    - --log-level: logging verbosity (default INFO).
    """
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help=f"Directory for caches (default: PROFIT_CACHE_* + '/{cache_help_subdir}')",
    )
    parser.add_argument(
        "--store-path",
        type=Path,
        default=None,
        help=f"Path to ColumnarSqliteStore (default: PROFIT_DATA_ROOT/{default_store_filename})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING...). Default: INFO",
    )
    if include_catalog_path:
        parser.add_argument(
            "--catalog-path",
            type=Path,
            default=None,
            help=f"Path to catalog SQLite DB (default: PROFIT_DATA_ROOT/{default_catalog_filename} or PROFIT_CATALOG_PATH)",
        )
    return parser
