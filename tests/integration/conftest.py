"""Shared fixtures for integration tests (dbt profiles, temp DuckDB, DNS)."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _public_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "oddsfox.resources.outbound_url.socket.getaddrinfo",
        lambda *a, **k: [(None, None, None, None, ("93.184.216.34", 443))],
    )


@pytest.fixture
def dbt_profiles_dir(tmp_path: Path) -> Path:
    """Empty profiles directory; tests write profiles.yml after choosing db_path."""
    profiles_dir = tmp_path / ".dbt"
    profiles_dir.mkdir()
    return profiles_dir


def write_dbt_profile(profiles_dir: Path, db_path: Path, *, threads: int = 2) -> None:
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: {threads}
  target: dev
""".strip()
        + "\n"
    )
