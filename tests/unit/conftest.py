"""Shared fixtures for unit tests (isolated env, temp DuckDB, deterministic time)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import requests

from oddsfox.resources.outbound_url import clear_outbound_url_host_cache

_NETWORK_DISABLED_MSG = (
    "Real outbound HTTP is disabled in unit tests. "
    "Inject a mock client or patch the transport at the module under test."
)


@pytest.fixture(autouse=True)
def stub_outbound_url_dns(monkeypatch):
    """Avoid real DNS lookups during unit tests; override per test when needed."""
    monkeypatch.setattr(
        "oddsfox.resources.outbound_url.socket.getaddrinfo",
        lambda *a, **k: [(None, None, None, None, ("93.184.216.34", 443))],
    )


@pytest.fixture(autouse=True)
def clear_outbound_url_dns_cache():
    """Prevent cross-test pollution from cached public DNS validation."""
    clear_outbound_url_host_cache()
    yield
    clear_outbound_url_host_cache()


@pytest.fixture(autouse=True)
def block_real_http(monkeypatch):
    """Fail fast when a unit test accidentally reaches the real network."""

    def _blocked_request(self, method, url, *args, **kwargs):
        del self, method, args, kwargs
        raise RuntimeError(f"{_NETWORK_DISABLED_MSG} ({url})")

    monkeypatch.setattr(requests.sessions.Session, "request", _blocked_request)


@pytest.fixture
def isolated_env(monkeypatch, tmp_path):
    """Clear relevant env vars and point DUCKDB at tmp_path for reload tests."""
    real_exists = Path.exists

    def _no_repo_dotenv(self: Path) -> bool:
        # Avoid loading repo-root `.env` on `reload_all_settings_modules()` so tests
        # see code defaults + explicit monkeypatch values only.
        if self.name == ".env":
            return False
        return real_exists(self)

    monkeypatch.setattr(Path, "exists", _no_repo_dotenv)

    for key in (
        "DUCKDB_NAME",
        "DUCKDB_PATH",
        "DBT_PROFILES_DIR",
        "ODDS_REQUESTS_PER_SECOND",
        "MARKETS_REQUESTS_PER_SECOND",
        "HTTP_CONNECT_TIMEOUT_SECONDS",
        "HTTP_READ_TIMEOUT_SECONDS",
        "CLOB_API_KEY",
        "CLOB_API_SECRET",
        "CLOB_API_PASSPHRASE",
        "INTERNATIONAL_RESULTS_ENABLED",
        "ELORATINGS_ENABLED",
        "CLUBELO_ENABLED",
    ):
        monkeypatch.delenv(key, raising=False)
    db = tmp_path / "test.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db))
    return db


@pytest.fixture
def reload_settings(isolated_env):
    """Reload settings + connection after env changes (per-test process isolation under xdist)."""
    from oddsfox.config._reload_settings import reload_all_settings_modules

    yield reload_all_settings_modules()


@pytest.fixture
def reset_connection_globals():
    """Reset DuckDB connection module globals between tests that mutate them."""
    import oddsfox.storage.duckdb.connection as connection

    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    yield
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False


@pytest.fixture
def no_sleep():
    with patch("time.sleep", lambda *_a, **_k: None):
        yield


@pytest.fixture
def patch_duckdb_connect_for_lock(monkeypatch):
    """Force duckdb.connect to raise IOError with lock message once, then succeed."""
    import duckdb as duckdb_mod

    calls = {"n": 0}

    def fake_connect(path, *a, **k):
        if calls["n"] == 0:
            calls["n"] += 1
            raise duckdb_mod.IOException("Conflicting lock")
        return duckdb_mod.connect(path, *a, **k)

    monkeypatch.setenv("PYTEST_CURRENT_TEST", "unit")
    return monkeypatch.setattr(duckdb_mod, "connect", fake_connect)
