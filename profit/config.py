from __future__ import annotations

import os
from pathlib import Path
from dataclasses import dataclass
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
    parser.add_argument(
        "--refresh-catalog",
        action="store_true",
        help="Force catalog refresh before fetching.",
    )
    return parser


@dataclass(frozen=True)
class ProfitConfig:
    data_root: Path
    cache_root: Path
    store_path: Path
    log_level: str
    refresh_catalog: bool

    @classmethod
    def from_args(cls, args) -> "ProfitConfig":
        ensure_profit_conf_loaded()
        data_root = get_data_root()
        cache_root = Path(getattr(args, "cache_dir") or get_cache_root(args=args))
        store_path = Path(getattr(args, "store_path") or get_columnar_db_path(args=args))
        log_level = getattr(args, "log_level", "INFO")
        refresh_catalog = bool(getattr(args, "refresh_catalog", False))
        return cls(
            data_root=data_root,
            cache_root=cache_root,
            store_path=store_path,
            log_level=log_level,
            refresh_catalog=refresh_catalog,
        )


    def apply_runtime_env(cfg: ProfitConfig) -> None:
        """
        Apply common runtime environment variables based on resolved config.
        """
        if cfg.refresh_catalog:
            os.environ["PROFIT_REFRESH_CATALOG"] = "1"

        yf_cache_dir = cfg.cache_root / "yfinance"
        os.environ.setdefault("YFINANCE_CACHE_DIR", str(yf_cache_dir))
        yf_cache_dir.mkdir(parents=True, exist_ok=True)

    # Convenience helpers so callers can avoid global functions.
    @staticmethod
    def resolve_data_root() -> Path:
        return get_data_root()

    @staticmethod
    def resolve_cache_root(*, args=None) -> Path:
        return get_cache_root(args=args)

    @staticmethod
    def resolve_columnar_db_path(*, args=None, filename: str = "columnar.sqlite3") -> Path:
        return get_columnar_db_path(args=args, filename=filename)
