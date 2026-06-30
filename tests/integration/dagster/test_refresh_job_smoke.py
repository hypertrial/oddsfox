from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from dagster import materialize
from dagster_dbt import DbtCliResource

import oddsfox.storage.duckdb.connection as connection
from oddsfox.config.settings import resolve_dbt_executable
from oddsfox.orchestration.assets import (
    DBT_PROJECT,
    polymarket_dbt,
    polymarket_market_metadata_backfill,
    polymarket_markets_snapshot,
    polymarket_token_odds_history,
    polymarket_wc2026_registry,
)
from oddsfox.storage.duckdb.wc2026_registry import (
    RegistryRow,
    upsert_registry_rows,
)


@pytest.fixture
def reset_connection_globals():
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False
    yield
    connection._SCHEMA_LOGGED = False
    connection._SCHEMA_INITIALIZED = False


@pytest.fixture
def no_sleep():
    with patch("time.sleep", lambda *_args, **_kwargs: None):
        yield


def _fake_sync_wc2026_registry(**kwargs):
    del kwargs
    upsert_registry_rows(
        [
            RegistryRow(
                "m1",
                "2026-fifa-world-cup-winner-595",
                "ev-smoke",
                "seed",
            )
        ]
    )
    return {"registry_rows_upserted": 1, "discovered_event_slugs": []}


def _materialize_refresh_path(
    monkeypatch,
    tmp_path: Path,
    *,
    db_name: str,
    slug: str,
    question: str,
    transient_token: str | None,
) -> Path:
    db_path = tmp_path / db_name
    profiles_dir = tmp_path / f"profiles-{db_name}"
    profiles_dir.mkdir()
    (profiles_dir / "profiles.yml").write_text(
        f"""
oddsfox:
  outputs:
    dev:
      type: duckdb
      path: {db_path}
      schema: dbt
      threads: 2
  target: dev
"""
    )

    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
    connection._SCHEMA_INITIALIZED = False
    connection._SCHEMA_LOGGED = False
    connection._ACTIVE_DUCKDB_PATH = Path(db_path)

    market_page = [
        {
            "id": "m1",
            "question": question,
            "category": "World Cup 2026 Testing",
            "description": "Synthetic World Cup 2026 end-to-end run",
            "outcomes": ["Yes", "No"],
            "volumeNum": 123.45,
            "active": True,
            "closed": False,
            "createdAt": "2026-04-13T10:00:00.000Z",
            "endDate": "2026-07-19T10:00:00.000Z",
            "clobTokenIds": ["t1", "t2"],
            "slug": slug,
            "events": [{"slug": "2026-fifa-world-cup-winner-595", "id": "ev-smoke"}],
        }
    ]

    def fake_refresh_registry_and_collect_markets_targeted(
        client, config, progress_callback=None
    ):
        del client, config
        if progress_callback:
            progress_callback(
                "wc2026_event_by_slug",
                {"slug": "2026-fifa-world-cup-winner-595", "found": True},
            )
        return (
            {
                "registry_rows_upserted": 1,
                "discovered_event_slugs": ["2026-fifa-world-cup-winner-595"],
                "registry_refreshed": True,
            },
            market_page,
            {
                "events_pages": 0,
                "markets_collected": len(market_page),
                "registry_refreshed": True,
                "api_requests": 2,
            },
        )

    def fake_fetch_token_history_with_retry(
        client,
        token_id,
        start_ts=None,
        end_ts=None,
        fidelity=1440,
        now_ts=None,
        **kwargs,
    ):
        del client, fidelity, kwargs
        if transient_token is not None and str(token_id) == transient_token:
            return None
        base_ts = int(
            start_ts if start_ts is not None else now_ts if now_ts is not None else 0
        )
        return [
            (str(token_id), base_ts + 60, 0.55),
            (str(token_id), base_ts + 120, 0.60),
        ]

    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.markets.sync.refresh_registry_and_collect_markets_targeted",
        fake_refresh_registry_and_collect_markets_targeted,
    )

    def _skip_backfill(task_name: str):
        def _skip(**kwargs):
            del kwargs
            return {"task": task_name, "skipped": True}

        return _skip

    for task in (
        "backfill_tokens",
        "backfill_slugs",
        "backfill_event_slugs",
        "backfill_end_dates",
    ):
        monkeypatch.setattr(
            f"oddsfox.orchestration.polymarket_ops.{task}",
            _skip_backfill(task),
        )
    monkeypatch.setattr(
        "oddsfox.ingestion.polymarket.odds.sync.fetch_token_history_with_retry",
        fake_fetch_token_history_with_retry,
    )
    monkeypatch.setattr(
        "oddsfox.orchestration.polymarket_ops.sync_wc2026_registry",
        _fake_sync_wc2026_registry,
    )

    result = materialize(
        [
            polymarket_markets_snapshot,
            polymarket_wc2026_registry,
            polymarket_market_metadata_backfill,
            polymarket_token_odds_history,
            polymarket_dbt,
        ],
        resources={
            "dbt": DbtCliResource(
                project_dir=DBT_PROJECT,
                profiles_dir=str(profiles_dir),
                dbt_executable=resolve_dbt_executable(),
            )
        },
        run_config={
            "ops": {
                "polymarket_markets_snapshot": {
                    "config": {
                        "discovery_mode": "targeted",
                    }
                },
                "polymarket_token_odds_history": {
                    "config": {
                        "workers": 1,
                        "batch_size": 1000,
                        "requests_per_second": 1,
                        "skip_recent_minutes": 0,
                        "overlap_minutes": 0,
                        "window_hours": 1,
                        "market_page_size": 100,
                        "progress_log_interval_tokens": 1,
                        "progress_log_interval_seconds": 1,
                        "no_progress_soft_timeout_seconds": 120,
                        "no_progress_hard_timeout_seconds": 600,
                        "progress_poll_seconds": 1,
                    }
                },
                "polymarket_dbt": {
                    "config": {
                        "progress_log_interval_events": 1,
                        "progress_log_interval_seconds": 1,
                        "no_progress_soft_timeout_seconds": 120,
                        "no_progress_hard_timeout_seconds": 600,
                        "progress_poll_seconds": 1,
                    }
                },
            }
        },
    )
    assert result.success is True
    return db_path


def test_refresh_path_materializes(
    monkeypatch,
    tmp_path,
    reset_connection_globals,
    no_sleep,
) -> None:
    slug = "world-cup-2026-smoke-pipeline-pass"
    question = "Will the World Cup 2026 smoke pipeline pass?"
    db_path = _materialize_refresh_path(
        monkeypatch,
        tmp_path,
        db_name=f"pipeline-{slug}.duckdb",
        slug=slug,
        question=question,
        transient_token=None,
    )
    with duckdb.connect(str(db_path)) as conn:
        checks = (
            conn.execute('select count(*) from "polymarket_raw"."markets"').fetchone()
            == (1,),
            conn.execute(
                'select count(*) from "polymarket_raw"."market_tokens"'
            ).fetchone()
            == (1,),
            conn.execute(
                'select count(*) from "polymarket_raw"."odds_history"'
            ).fetchone()[0]
            > 0,
            conn.execute(
                'select count(*) from "polymarket_raw"."token_odds_daily"'
            ).fetchone()[0]
            > 0,
            conn.execute(
                "select count(*) from polymarket_marts.market_coverage"
            ).fetchone()
            == (1,),
            conn.execute(
                "select count(*) from polymarket_marts.token_coverage "
                "where max_gap_days >= 0 and market_token_count >= market_fully_checked_tokens"
            ).fetchone()
            == (2,),
        )
        assert all(checks)
