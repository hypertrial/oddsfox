"""Warehouse paths, dotenv bootstrap, DuckDB/dbt dirs."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

PACKAGE_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = PACKAGE_DIR.parent
BASE_DIR = SRC_DIR.parent

env_path = BASE_DIR / ".env"
if env_path.exists():
    load_dotenv(env_path)

DUCKDB_NAME = os.getenv("DUCKDB_NAME", "oddsfox.duckdb")
DUCKDB_PATH = (BASE_DIR / DUCKDB_NAME).resolve()
os.environ.setdefault("DUCKDB_PATH", str(DUCKDB_PATH))

DBT_PROJECT_DIR = BASE_DIR / "dbt"
_DEFAULT_DBT_PROFILES_DIR = BASE_DIR / "dbt" / "profiles"
_ENV_DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
DBT_PROFILES_DIR = (
    Path(_ENV_DBT_PROFILES_DIR) if _ENV_DBT_PROFILES_DIR else _DEFAULT_DBT_PROFILES_DIR
)
_profiles_yml = DBT_PROFILES_DIR / "profiles.yml"
if not _profiles_yml.exists() or "oddsfox:" not in _profiles_yml.read_text():
    DBT_PROFILES_DIR = _DEFAULT_DBT_PROFILES_DIR
os.environ["DBT_PROFILES_DIR"] = str(DBT_PROFILES_DIR)


def dbt_cli_argv(*args: str) -> list[str]:
    """Invoke dbt with the active Python interpreter (project venv when used via make)."""
    return [sys.executable, "-m", "dbt.cli.main", *args]


def resolve_dbt_executable() -> str:
    """Prefer the dbt console script next to the active interpreter, else PATH."""
    venv_dbt = Path(sys.executable).with_name("dbt")
    if venv_dbt.is_file():
        return str(venv_dbt)
    return shutil.which("dbt") or "dbt"


__all__ = [
    "BASE_DIR",
    "DBT_PROFILES_DIR",
    "DBT_PROJECT_DIR",
    "DUCKDB_NAME",
    "DUCKDB_PATH",
    "PACKAGE_DIR",
    "SRC_DIR",
    "dbt_cli_argv",
    "resolve_dbt_executable",
]
