import pytest

pytest.importorskip("dagster")
pytest.importorskip("dagster_dbt")

from unittest.mock import MagicMock

from oddsfox.orchestration import config as orch_config
from oddsfox.orchestration import dbt_build as dbt_build_mod
from tests.unit.orchestration.orchestration_test_support import (
    _DormantThread,
    _FakeClock,
    _FakeQueue,
    _ImmediateThread,
    _patch_guardrail_clock,
)


def test_dbt_assets_definition_streams_build_events(monkeypatch):
    from oddsfox.orchestration.assets import polymarket_dbt

    monkeypatch.setattr(
        "oddsfox.orchestration.polymarket_ops.delete_orphan_market_tokens",
        lambda: 0,
    )

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock()
            m.stream = lambda: iter(["event"])
            return m

    fn = polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    events = list(fn(ctx, MockDbt(), orch_config.DbtBuildConfig()))
    assert events == ["event"]


def test_dbt_assets_logs_when_orphan_market_tokens_removed(monkeypatch):
    from oddsfox.orchestration.assets import polymarket_dbt

    monkeypatch.setattr(
        "oddsfox.orchestration.polymarket_ops.delete_orphan_market_tokens",
        lambda: 2,
    )

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock()
            m.stream = lambda: iter([])
            return m

    fn = polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    list(fn(ctx, MockDbt(), orch_config.DbtBuildConfig()))
    joined = " ".join(str(c) for c in ctx.log.info.call_args_list)
    assert "orphan market_tokens" in joined


def test_dbt_assets_guardrail_hard_timeout_terminates_process(monkeypatch):
    from oddsfox.orchestration import assets as assets_mod
    from oddsfox.orchestration.assets import polymarket_dbt

    monkeypatch.setattr(
        "oddsfox.orchestration.polymarket_ops.delete_orphan_market_tokens",
        lambda: 0,
    )
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _DormantThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )

    process_mock = MagicMock(returncode=None)

    class MockDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=process_mock)
            m.stream = lambda: iter(())
            return m

    fn = polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    with pytest.raises(Exception):
        list(
            fn(
                ctx,
                MockDbt(),
                orch_config.DbtBuildConfig(
                    no_progress_soft_timeout_seconds=None,
                    no_progress_hard_timeout_seconds=1,
                    progress_log_interval_seconds=1,
                    progress_poll_seconds=1,
                ),
            )
        )
    assert process_mock.terminate.called


def test_dbt_assets_guardrail_wait_continue_and_stream_error(monkeypatch):
    from oddsfox.orchestration import assets as assets_mod
    from oddsfox.orchestration.assets import polymarket_dbt

    monkeypatch.setattr(
        "oddsfox.orchestration.polymarket_ops.delete_orphan_market_tokens",
        lambda: 0,
    )
    fn = polymarket_dbt.op.compute_fn.decorated_fn
    ctx = MagicMock()
    clock = _FakeClock()
    _patch_guardrail_clock(monkeypatch, assets_mod, clock)
    monkeypatch.setattr(dbt_build_mod, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        dbt_build_mod,
        "Queue",
        lambda *args, **kwargs: _FakeQueue(
            *args,
            **kwargs,
            clock=clock,
            empty_cycles=1,
            empty_advance=1.1,
        ),
    )

    class SlowThenEventDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=None))
            m.stream = lambda: iter(["event"])
            return m

    events = list(
        fn(
            ctx,
            SlowThenEventDbt(),
            orch_config.DbtBuildConfig(
                no_progress_soft_timeout_seconds=None,
                no_progress_hard_timeout_seconds=None,
                progress_log_interval_seconds=1,
                progress_poll_seconds=1,
            ),
        )
    )
    assert events == ["event"]

    class ErrorStreamDbt:
        def cli(self, *a, **k):
            m = MagicMock(process=MagicMock(returncode=1))

            def _stream():
                raise RuntimeError("dbt stream blew up")
                yield  # pragma: no cover

            m.stream = _stream
            return m

    with pytest.raises(RuntimeError, match="dbt stream blew up"):
        list(fn(MagicMock(), ErrorStreamDbt(), orch_config.DbtBuildConfig()))


def test_prepare_dbt_project_warns_when_prepare_fails_but_manifest_exists(
    tmp_path, caplog
):
    import logging

    pytest.importorskip("dagster_dbt")

    from oddsfox.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    class FakePreparer:
        def using_dagster_dev(self):
            return True

        def prepare_if_dev(self, _project):
            raise RuntimeError("prepare failed")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    caplog.set_level(logging.WARNING)
    project = FakeProject()
    dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)
    assert any("prepare_if_dev() failed" in r.getMessage() for r in caplog.records)


def test_prepare_dbt_project_reraises_when_prepare_fails_and_manifest_missing(tmp_path):
    pytest.importorskip("dagster_dbt")

    from oddsfox.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "nonexistent_manifest.json"

    class FakePreparer:
        def using_dagster_dev(self):
            return True

        def prepare_if_dev(self, _project):
            raise RuntimeError("prepare failed")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    project = FakeProject()
    with pytest.raises(RuntimeError, match="prepare failed"):
        dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)


def test_prepare_dbt_project_prepares_manifest_outside_dagster_dev_when_missing(
    tmp_path,
):
    pytest.importorskip("dagster_dbt")

    from oddsfox.orchestration import dbt_project as dbt_project_mod

    manifest = tmp_path / "manifest.json"
    prepared: list[str] = []

    class FakePreparer:
        def using_dagster_dev(self):
            return False

        def prepare(self, project):
            prepared.append(str(project.manifest_path))
            manifest.write_text("{}")

    class FakeProject:
        manifest_path = manifest
        preparer = FakePreparer()

    project = FakeProject()
    dbt_project_mod.prepare_dbt_project(project, preparer=project.preparer)
    assert prepared == [str(manifest)]
    assert manifest.exists()


def test_oddsfox_dbt_project_preparer_uses_resolved_executable(monkeypatch):
    pytest.importorskip("dagster_dbt")

    from oddsfox.orchestration.dbt_project import OddsfoxDbtProjectPreparer

    captured: list[str] = []

    class FakeDbtCliResource:
        def __init__(self, **kwargs):
            captured.append(kwargs["dbt_executable"])

        def cli(self, *_args, **_kwargs):
            return self

        def wait(self):
            return None

    monkeypatch.setattr(
        "oddsfox.orchestration.dbt_project.resolve_dbt_executable",
        lambda: "/venv/bin/dbt",
    )
    monkeypatch.setattr(
        "dagster_dbt.core.resource.DbtCliResource",
        FakeDbtCliResource,
    )

    preparer = OddsfoxDbtProjectPreparer()
    monkeypatch.setattr(
        preparer, "_invalidate_seeds_in_partial_parse", lambda _project: None
    )
    project = MagicMock(target_path=MagicMock(), profiles_dir="dbt/profiles")
    preparer._prepare_packages(project)
    preparer._prepare_manifest(project)
    assert captured == ["/venv/bin/dbt", "/venv/bin/dbt"]


def test_stream_dbt_build_appends_full_refresh_flag():
    from unittest.mock import MagicMock

    captured_args: list[list[str]] = []

    class MockDbt:
        def cli(self, args, context=None):
            captured_args.append(list(args))
            m = MagicMock()
            m.stream = lambda: iter(["event"])
            m.process = MagicMock(returncode=0)
            return m

    ctx = MagicMock()
    list(
        dbt_build_mod.stream_dbt_build(
            asset_name="polymarket_dbt",
            context=ctx,
            dbt=MockDbt(),
            config=orch_config.DbtBuildConfig(full_refresh=True),
        )
    )
    assert captured_args == [["build", "--full-refresh"]]
