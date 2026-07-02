import importlib
from unittest.mock import MagicMock

import duckdb
import pytest

import oddsfox.storage.duckdb.connection as connection
from oddsfox.config._reload_settings import reload_all_settings_modules
from oddsfox.storage.duckdb.schemas import polymarket as polymarket_schema


@pytest.fixture(autouse=True)
def reset_schema_globals():
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    yield
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False


def test_resolved_duckdb_path_absolute_env(monkeypatch, tmp_path, isolated_env):
    db = tmp_path / "abs.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    assert conn._resolved_duckdb_path() == db


def test_resolved_duckdb_path_relative_env(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", "rel.duckdb")

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    p = conn._resolved_duckdb_path()
    assert p.name == "rel.duckdb"
    assert p.is_absolute()


def test_resolved_duckdb_path_uses_duckdb_path_when_name_unset(monkeypatch, tmp_path):
    """Covers the ``return DUCKDB_PATH`` branch in ``_resolved_duckdb_path`` (no DUCKDB_NAME)."""
    import os

    settings = reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    real_getenv = os.getenv

    def _getenv(name: str, default=None) -> str | None:
        if name == "DUCKDB_NAME":
            return None
        return real_getenv(name, default)

    conn = importlib.reload(conn)
    monkeypatch.setattr(conn.os, "getenv", _getenv)
    p = conn._resolved_duckdb_path()
    assert p.resolve() == settings.DUCKDB_PATH.resolve()


def test_connect_ioerror_without_pytest_env_reraises(
    monkeypatch, tmp_path, isolated_env
):
    """IOException when not in pytest alternate-path should hit bare raise (45->66)."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "npy.duckdb"))

    def boom(*a, **k):
        raise duckdb.IOException("disk full")

    monkeypatch.setattr(duckdb, "connect", boom)

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    with pytest.raises(duckdb.IOException, match="disk full"):
        conn._connect_duckdb()


def test_connect_ioerror_non_lock_reraises(monkeypatch, tmp_path, isolated_env):
    """IOException without lock wording should re-raise (line 66 bare raise)."""
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "nl.duckdb"))
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "unit")

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)

    def boom(*a, **k):
        raise duckdb.IOException("disk full")

    monkeypatch.setattr(duckdb, "connect", boom)
    with pytest.raises(duckdb.IOException, match="disk full"):
        conn._connect_duckdb()


def test_is_duckdb_lock_io_error_detects_lock_messages():
    assert connection.is_duckdb_lock_io_error(
        duckdb.IOException("Conflicting lock on file")
    )
    assert connection.is_duckdb_lock_io_error(
        duckdb.IOException("could not set lock on file")
    )
    assert not connection.is_duckdb_lock_io_error(duckdb.IOException("disk full"))
    assert not connection.is_duckdb_lock_io_error(RuntimeError("other"))


def test_open_writable_duckdb_connection_retries_then_succeeds(monkeypatch, tmp_path):
    db_path = tmp_path / "w.duckdb"
    calls: list[int] = []
    real_connect = duckdb.connect

    def fake_connect(path, *a, **k):
        calls.append(1)
        if len(calls) == 1:
            raise duckdb.IOException("Conflicting lock on file")
        return real_connect(str(path))

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    monkeypatch.setattr(connection.time, "sleep", lambda _: None)
    conn = connection.open_writable_duckdb_connection(
        db_path, attempts=3, base_sleep_seconds=0.01
    )
    try:
        assert conn.execute("select 1").fetchone()[0] == 1
        assert len(calls) == 2
    finally:
        conn.close()


def test_open_writable_duckdb_connection_requires_positive_attempts(tmp_path):
    with pytest.raises(ValueError, match="attempts must be"):
        connection.open_writable_duckdb_connection(tmp_path / "x.duckdb", attempts=0)


def test_open_writable_duckdb_connection_first_try_success(tmp_path):
    db_path = tmp_path / "ok.duckdb"
    conn = connection.open_writable_duckdb_connection(db_path, attempts=1)
    try:
        assert conn.execute("select 1").fetchone()[0] == 1
    finally:
        conn.close()


def test_open_writable_duckdb_connection_exhausts_lock_retries(monkeypatch, tmp_path):
    db_path = tmp_path / "locked.duckdb"

    def always_lock(*args, **kwargs):
        raise duckdb.IOException("Conflicting lock on file")

    monkeypatch.setattr(duckdb, "connect", always_lock)
    monkeypatch.setattr(connection.time, "sleep", lambda _: None)
    with pytest.raises(duckdb.IOException, match="Conflicting lock"):
        connection.open_writable_duckdb_connection(db_path, attempts=3)


def test_open_writable_duckdb_connection_non_lock_raises_immediately(
    monkeypatch, tmp_path
):
    db_path = tmp_path / "e.duckdb"

    def fake_connect(*a, **k):
        raise duckdb.IOException("disk full")

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    with pytest.raises(duckdb.IOException, match="disk full"):
        connection.open_writable_duckdb_connection(db_path, attempts=3)


def test_connect_lock_fallback(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "w.duckdb"))
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "unit")

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)

    calls = {"n": 0}
    real_connect = duckdb.connect

    def fake_connect(path, *a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise duckdb.IOException("Conflicting lock")
        return real_connect(path, *a, **k)

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw7")
    c = conn._connect_duckdb()
    c.execute("select 1")
    c.close()


def test_connect_lock_fallback_retries_unique_temp_paths(
    monkeypatch, tmp_path, isolated_env
):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "w.duckdb"))
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "unit")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw6")

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    real_connect = duckdb.connect
    attempted: list[str] = []

    def fake_connect(path, *a, **k):
        attempted.append(str(path))
        if len(attempted) == 1:
            raise duckdb.IOException("Conflicting lock")
        if len(attempted) == 2:
            raise duckdb.IOException("Conflicting lock")
        return real_connect(path, *a, **k)

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    c = conn._connect_duckdb()
    try:
        assert c.execute("select 1").fetchone()[0] == 1
    finally:
        c.close()
    assert attempted[1] != attempted[2]


def test_connect_lock_fallback_retry_non_lock_reraises(
    monkeypatch, tmp_path, isolated_env
):
    """Alt-path connect raises a non-lock IOException; must propagate (107->108)."""
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "w.duckdb"))
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "unit")

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)

    calls = {"n": 0}

    def fake_connect(path, *a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise duckdb.IOException("Conflicting lock")
        raise duckdb.IOException("disk full")

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    with pytest.raises(duckdb.IOException, match="disk full"):
        conn._connect_duckdb()


def test_connect_lock_fallback_exhausted_retries_reraises(
    monkeypatch, tmp_path, isolated_env
):
    """After repeated lock errors on temp paths, re-raise the outer lock error (86->109)."""
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "w.duckdb"))
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "unit")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw5")

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)

    def fake_connect(path, *a, **k):
        raise duckdb.IOException("Conflicting lock")

    monkeypatch.setattr(duckdb, "connect", fake_connect)
    with pytest.raises(duckdb.IOException, match="Conflicting lock"):
        conn._connect_duckdb()


def test_init_duck_db_idempotent(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "db.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    conn.init_duck_db()
    conn.init_duck_db()
    assert conn._SCHEMA_INITIALIZED is True
    with conn.get_connection() as c:
        names = frozenset(
            r[0]
            for r in c.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema IN ('polymarket_raw', 'polymarket_ops')
                """
            ).fetchall()
        )
    assert "market_tokens" in names
    assert "markets" not in names


def test_ensure_duck_db_sets_active_path(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "e.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    conn.ensure_duck_db()
    assert conn._ACTIVE_DUCKDB_PATH.name == "e.duckdb"


def test_ensure_duck_db_switches_active_path_without_manual_reset(
    monkeypatch, tmp_path, isolated_env
):
    first = tmp_path / "first.duckdb"
    second = tmp_path / "second.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(first))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    conn.ensure_duck_db()
    assert conn._ACTIVE_DUCKDB_PATH == first
    assert conn._SCHEMA_INITIALIZED is True

    monkeypatch.setenv("DUCKDB_NAME", str(second))
    conn.ensure_duck_db()

    assert conn._ACTIVE_DUCKDB_PATH == second
    assert conn._SCHEMA_INITIALIZED is True
    with duckdb.connect(str(second)) as c:
        table_count = c.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema IN ('polymarket_raw', 'polymarket_ops')
            """
        ).fetchone()[0]
    assert table_count > 0


def test_get_connection_retries_transient_lock(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "retry.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    real_connect = duckdb.connect
    calls = {"n": 0}

    def flaky_connect(path, *args, read_only=False, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise duckdb.IOException("Could not set lock on file")
        return real_connect(path, *args, read_only=read_only, **kwargs)

    monkeypatch.setattr(duckdb, "connect", flaky_connect)
    conn.ensure_duck_db()
    with conn.get_connection() as c:
        assert c.execute("select 1").fetchone()[0] == 1
    assert calls["n"] == 3


def test_get_connection_and_persistent(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "p.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    conn.ensure_duck_db()
    with conn.get_connection() as c:
        assert c.execute("select 1").fetchone()[0] == 1
    pc = conn.get_persistent_connection()
    try:
        assert pc.execute("select 2").fetchone()[0] == 2
    finally:
        pc.close()


def test_use_conn_with_explicit_connection(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "u.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    conn.ensure_duck_db()
    direct = duckdb.connect(str(tmp_path / "u.duckdb"))
    try:
        with conn._use_conn(direct) as c:
            assert c is direct
    finally:
        direct.close()


def test_use_conn_implicit_opens_temporary_connection(
    monkeypatch, tmp_path, isolated_env
):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "uc2.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    conn.ensure_duck_db()
    with conn._use_conn(None) as c:
        assert c.execute("select 1").fetchone()[0] == 1


def test_init_duck_db_debug_log_when_schema_logged_already(
    monkeypatch, tmp_path, isolated_env
):
    """Hit logger.debug branch when _SCHEMA_LOGGED True but schema not initialized."""
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "dbg.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    conn._SCHEMA_LOGGED = True
    conn._SCHEMA_INITIALIZED = False
    conn.init_duck_db()
    assert conn._SCHEMA_INITIALIZED is True


def test_create_indexes_swallows_errors(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "idx.duckdb"))
    reload_all_settings_modules()
    conn = importlib.reload(connection)

    conn.init_duck_db()
    bad = MagicMock()

    def _execute_side_effect(sql, *args, **kwargs):
        if "information_schema.tables" in str(sql):
            result = MagicMock()
            result.fetchone.return_value = (0,)
            return result
        raise RuntimeError("no index")

    bad.execute.side_effect = _execute_side_effect
    polymarket_schema.ensure_polymarket_indexes(bad)
    assert bad.execute.called


def _odds_history_index_names(c: duckdb.DuckDBPyConnection) -> set[str]:
    rows = c.execute(
        """
        SELECT index_name
        FROM duckdb_indexes()
        WHERE schema_name = 'polymarket_raw' AND table_name = 'odds_history'
        """
    ).fetchall()
    return {str(name) for (name,) in rows}


def _odds_history_primary_key_columns(c: duckdb.DuckDBPyConnection) -> list[str] | None:
    rows = c.execute(
        """
        SELECT constraint_type, constraint_column_names
        FROM duckdb_constraints()
        WHERE schema_name = 'polymarket_raw' AND table_name = 'odds_history'
        """
    ).fetchall()
    for constraint_type, columns in rows:
        if constraint_type == "PRIMARY KEY":
            return list(columns)
    return None


def test_init_duck_db_uses_odds_history_primary_key_only(
    monkeypatch, tmp_path, isolated_env
):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "fresh.duckdb"))
    reload_all_settings_modules()
    conn = importlib.reload(connection)
    conn.init_duck_db()

    with duckdb.connect(str(tmp_path / "fresh.duckdb")) as c:
        names = _odds_history_index_names(c)
        assert "idx_odds_token" not in names
        assert "idx_odds_timestamp" not in names
        assert _odds_history_primary_key_columns(c) == ["clobTokenId", "timestamp"]


def test_init_duck_db_swallows_alter_table_error(monkeypatch, tmp_path, isolated_env):
    monkeypatch.setenv("DUCKDB_NAME", str(tmp_path / "alter.duckdb"))

    reload_all_settings_modules()
    import oddsfox.storage.duckdb.connection as conn

    conn = importlib.reload(conn)
    real_connect = conn._connect_duckdb

    class Wrapper:
        def __init__(self, inner):
            self.inner = inner
            self.alter_calls = 0

        def execute(self, sql, *args, **kwargs):
            if "ADD COLUMN IF NOT EXISTS end_date TIMESTAMP" in sql:
                self.alter_calls += 1
                raise RuntimeError("already exists")
            if "ADD COLUMN IF NOT EXISTS event_id TEXT" in sql:
                self.alter_calls += 1
                raise RuntimeError("already exists")
            return self.inner.execute(sql, *args, **kwargs)

        def close(self):
            return self.inner.close()

    monkeypatch.setattr(
        conn, "_connect_duckdb", lambda path=None: Wrapper(real_connect(path))
    )
    conn.init_duck_db()
    assert conn._SCHEMA_INITIALIZED is True


def test_connect_explicit_path_skips_global_active(monkeypatch, tmp_path, isolated_env):
    p = tmp_path / "explicit.duckdb"
    import oddsfox.storage.duckdb.connection as conn

    c = conn._connect_duckdb(p)
    try:
        assert c.execute("select 1").fetchone()[0] == 1
    finally:
        c.close()

