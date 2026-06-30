import logging
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import duckdb

from oddsfox.config.settings import BASE_DIR, DUCKDB_PATH
from oddsfox.storage.duckdb.schemas.constants import (
    POLYMARKET_OPS_SCHEMA,
    POLYMARKET_RAW_SCHEMA,
    polymarket_ops_tbl,
    polymarket_q,
    polymarket_raw_tbl,
)
from oddsfox.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
    create_test_markets_table,
    drop_legacy_bootstrap_markets_table_if_needed,
    ensure_polymarket_indexes,
)

logger = logging.getLogger(__name__)

_SCHEMA_LOGGED = False
_SCHEMA_INITIALIZED = False
_ACTIVE_DUCKDB_PATH: Path = DUCKDB_PATH


def _resolved_duckdb_path() -> Path:
    env_name = os.getenv("DUCKDB_NAME")
    if env_name:
        env_path = Path(env_name)
        return env_path if env_path.is_absolute() else BASE_DIR / env_name
    return DUCKDB_PATH


def _connect_duckdb(
    path: Optional[Path] = None, *, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    global _ACTIVE_DUCKDB_PATH
    if path is None:
        _ACTIVE_DUCKDB_PATH = _resolved_duckdb_path()
        path = _ACTIVE_DUCKDB_PATH

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        return duckdb.connect(str(path), read_only=read_only)
    except duckdb.IOException as exc:
        if os.getenv("PYTEST_CURRENT_TEST") and is_duckdb_lock_io_error(exc):
            worker = os.getenv("PYTEST_XDIST_WORKER", "gw0")
            tmp_dir = Path(tempfile.gettempdir())
            for attempt in range(10):
                suffix = "" if attempt == 0 else f".{attempt}"
                alt_path = (
                    tmp_dir
                    / f"{path.stem}.pytest.{worker}.{os.getpid()}{suffix}{path.suffix}"
                )
                logger.warning(
                    "DuckDB locked at %s; using temporary test DB at %s",
                    path,
                    alt_path,
                )
                _ACTIVE_DUCKDB_PATH = alt_path
                try:
                    return duckdb.connect(str(_ACTIVE_DUCKDB_PATH))
                except duckdb.IOException as retry_exc:
                    if not is_duckdb_lock_io_error(retry_exc):
                        raise
        raise


def is_duckdb_lock_io_error(exc: BaseException) -> bool:
    if not isinstance(exc, duckdb.IOException):
        return False
    msg = str(exc).lower()
    return "conflicting lock" in msg or "could not set lock" in msg


def open_duckdb_connection(
    path: Optional[Path] = None, *, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    return _connect_duckdb(path=path, read_only=read_only)


def open_writable_duckdb_connection(
    path: Path,
    *,
    attempts: int = 12,
    base_sleep_seconds: float = 0.25,
) -> duckdb.DuckDBPyConnection:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(attempts):
        try:
            return duckdb.connect(str(path))
        except duckdb.IOException as exc:
            if not is_duckdb_lock_io_error(exc) or attempt == attempts - 1:
                raise
            time.sleep(min(base_sleep_seconds * (2**attempt), 4.0))


def init_duck_db() -> None:
    global _SCHEMA_LOGGED, _SCHEMA_INITIALIZED
    if _SCHEMA_INITIALIZED:
        return
    conn = open_writable_duckdb_connection(_resolved_duckdb_path())
    if not _SCHEMA_LOGGED:
        logger.info(
            "Ensuring DuckDB Polymarket schemas (%s, %s)",
            POLYMARKET_RAW_SCHEMA,
            POLYMARKET_OPS_SCHEMA,
        )
        _SCHEMA_LOGGED = True
    try:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{POLYMARKET_RAW_SCHEMA}"')
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{POLYMARKET_OPS_SCHEMA}"')
        bootstrap_polymarket_tables(conn)
        drop_legacy_bootstrap_markets_table_if_needed(conn)
        create_test_markets_table(conn)
        ensure_polymarket_indexes(conn)
        _SCHEMA_INITIALIZED = True
    finally:
        conn.close()


def ensure_duck_db() -> None:
    global _ACTIVE_DUCKDB_PATH
    if _SCHEMA_INITIALIZED:
        return
    _ACTIVE_DUCKDB_PATH = _resolved_duckdb_path()
    init_duck_db()


@contextmanager
def get_connection():
    ensure_duck_db()
    conn = open_writable_duckdb_connection(_ACTIVE_DUCKDB_PATH)
    try:
        yield conn
    finally:
        conn.close()


def get_persistent_connection() -> duckdb.DuckDBPyConnection:
    ensure_duck_db()
    return open_writable_duckdb_connection(_ACTIVE_DUCKDB_PATH)


@contextmanager
def _use_conn(conn=None):
    if conn is not None:
        yield conn
    else:
        with get_connection() as c:
            yield c


__all__ = [
    "POLYMARKET_OPS_SCHEMA",
    "POLYMARKET_RAW_SCHEMA",
    "_use_conn",
    "ensure_duck_db",
    "get_connection",
    "get_persistent_connection",
    "init_duck_db",
    "is_duckdb_lock_io_error",
    "open_duckdb_connection",
    "open_writable_duckdb_connection",
    "polymarket_ops_tbl",
    "polymarket_q",
    "polymarket_raw_tbl",
]
