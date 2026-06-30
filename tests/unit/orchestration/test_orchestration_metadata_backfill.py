import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from unittest.mock import patch

from oddsfox.orchestration import polymarket_ops as polymarket_ops_mod
from tests.unit.orchestration.orchestration_test_support import (
    _DelayedWorkerThread,
    _FakeClock,
    _patch_guardrail_clock,
)


def test_metadata_backfill_asset_invokes_progress_callback():
    from dagster import materialize

    from oddsfox.orchestration.assets import (
        polymarket_market_metadata_backfill,
    )

    op_key = polymarket_market_metadata_backfill.key.to_python_identifier()

    def combined(**kw):
        cb = kw.get("progress_callback")
        if cb:
            cb("probe_metadata", {"x": 1})
        return {"task": "backfill_market_metadata", "skipped": True}

    with patch(
        "oddsfox.orchestration.polymarket_ops.backfill_market_metadata",
        combined,
    ):
        materialize(
            [polymarket_market_metadata_backfill],
            run_config={
                "ops": {
                    op_key: {
                        "config": {
                            "include_slugs": True,
                            "include_event_slugs": True,
                            "include_end_dates": True,
                        }
                    }
                }
            },
        )


def test_metadata_backfill_config_branches():
    from dagster import materialize

    from oddsfox.orchestration.assets import (
        polymarket_market_metadata_backfill,
    )

    op_key = polymarket_market_metadata_backfill.key.to_python_identifier()
    cfg = {
        "batch_size": 20,
        "force": False,
        "include_slugs": False,
        "include_event_slugs": False,
        "include_end_dates": False,
    }
    with patch(
        "oddsfox.orchestration.polymarket_ops.backfill_market_metadata",
        lambda **k: {"task": "backfill_market_metadata", "skipped": True},
    ):
        materialize(
            [polymarket_market_metadata_backfill],
            run_config={"ops": {op_key: {"config": cfg}}},
        )

    cfg2 = {
        **cfg,
        "include_slugs": True,
        "include_event_slugs": False,
        "include_end_dates": True,
    }
    with patch(
        "oddsfox.orchestration.polymarket_ops.backfill_market_metadata",
        lambda **k: {"task": "backfill_market_metadata", "skipped": True},
    ):
        materialize(
            [polymarket_market_metadata_backfill],
            run_config={"ops": {op_key: {"config": cfg2}}},
        )


def test_metadata_backfill_forwards_event_slug_fallback_and_gamma_kwargs():
    from dagster import materialize

    from oddsfox.orchestration.assets import (
        polymarket_market_metadata_backfill,
    )

    op_key = polymarket_market_metadata_backfill.key.to_python_identifier()
    captured = {}

    def capture_metadata(**kw):
        captured.update(kw)
        return {"task": "backfill_market_metadata", "skipped": True}

    with patch(
        "oddsfox.orchestration.polymarket_ops.backfill_market_metadata",
        capture_metadata,
    ):
        materialize(
            [polymarket_market_metadata_backfill],
            run_config={
                "ops": {
                    op_key: {
                        "config": {
                            "include_slugs": True,
                            "include_event_slugs": True,
                            "include_end_dates": False,
                            "gamma_requests_per_second": 1.5,
                            "event_slug_fallback_max_pages": 42,
                            "event_slug_fallback_max_pages_without_progress": 5,
                            "event_slug_fallback_progress_pages": 7,
                            "progress_log_interval_batches": 3,
                        }
                    }
                }
            },
        )

    assert captured["gamma_requests_per_second"] == 1.5
    assert captured["progress_every_n_batches"] == 3
    assert captured["batch_size"] == 50
    assert captured["event_slug_fallback_max_pages"] == 42
    assert captured["event_slug_fallback_max_pages_without_progress"] == 5
    assert captured["event_slug_fallback_progress_every_pages"] == 7
    assert captured["include_event_slugs"] is True
    assert captured["include_end_dates"] is False
    assert captured["market_scope"] == "wc2026"
    assert callable(captured["progress_callback"])


def test_metadata_backfill_guardrail_poll_checks_and_raises_worker_errors(monkeypatch):
    from dagster import materialize

    from oddsfox.orchestration import assets as assets_mod
    from oddsfox.orchestration.assets import (
        polymarket_market_metadata_backfill,
    )

    op_key = polymarket_market_metadata_backfill.key.to_python_identifier()
    check_calls = {"count": 0}
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    real_check = polymarket_ops_mod.ProgressGuardrail.check

    def counting_check(self, *args, **kwargs):
        check_calls["count"] += 1
        return real_check(self, *args, **kwargs)

    monkeypatch.setattr(polymarket_ops_mod.ProgressGuardrail, "check", counting_check)
    monkeypatch.setattr(
        polymarket_ops_mod,
        "Thread",
        lambda *args, **kwargs: _DelayedWorkerThread(
            *args,
            **kwargs,
            clock=clock,
            advance_seconds=1.1,
        ),
    )

    with patch(
        "oddsfox.orchestration.polymarket_ops.backfill_market_metadata",
        lambda **k: {"task": "backfill_market_metadata", "ok": True},
    ):
        materialize(
            [polymarket_market_metadata_backfill],
            run_config={
                "ops": {
                    op_key: {
                        "config": {
                            "include_slugs": False,
                            "include_event_slugs": False,
                            "include_end_dates": False,
                            "progress_poll_seconds": 1,
                            "no_progress_soft_timeout_seconds": None,
                            "no_progress_hard_timeout_seconds": None,
                        }
                    }
                }
            },
        )

    assert check_calls["count"] >= 1

    with patch(
        "oddsfox.orchestration.polymarket_ops.backfill_market_metadata",
        lambda **k: (_ for _ in ()).throw(RuntimeError("boom phase")),
    ):
        with pytest.raises(RuntimeError, match="boom phase"):
            materialize(
                [polymarket_market_metadata_backfill],
                run_config={
                    "ops": {
                        op_key: {
                            "config": {
                                "include_slugs": False,
                                "include_event_slugs": False,
                                "include_end_dates": False,
                            }
                        }
                    }
                },
            )
