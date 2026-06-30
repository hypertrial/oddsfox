"""Coverage for pipeline_ops.sync_wc2026_registry."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from oddsfox.orchestration import pipeline_ops


def test_sync_wc2026_registry_delegates_to_refresh():
    client = MagicMock()
    seen = {}

    def _refresh(_client, **kwargs):
        seen.update(kwargs)
        return {"task": "refresh_wc2026_registry", "registry_rows_upserted": 1}

    with (
        patch(
            "oddsfox.orchestration.pipeline_ops.build_client",
            return_value=client,
        ),
        patch(
            "oddsfox.orchestration.pipeline_ops.refresh_registry_from_events",
            _refresh,
        ),
    ):
        out = pipeline_ops.sync_wc2026_registry(
            max_event_pages=3,
            progress_callback=lambda phase, payload: seen.setdefault(
                "progress", (phase, payload)
            ),
        )
    assert out["registry_rows_upserted"] == 1
    assert seen["max_pages"] == 3
    assert callable(seen["progress_callback"])
