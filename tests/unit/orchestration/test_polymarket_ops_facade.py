"""Polymarket ops facade re-exports match pipeline_ops implementations."""

from __future__ import annotations

import pytest

from oddsfox.orchestration import pipeline_ops, polymarket_ops

pytestmark = pytest.mark.facade


def test_polymarket_ops_facade_matches_pipeline_ops() -> None:
    for name in polymarket_ops.__all__:
        assert getattr(polymarket_ops, name) is getattr(pipeline_ops, name)
