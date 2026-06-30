"""Reload the full split settings subgraph (warehouse must re-run before derived modules)."""

from __future__ import annotations

import importlib
from types import ModuleType

_SETTINGS_CHAIN: tuple[str, ...] = (
    "oddsfox.config.settings_warehouse",
    "oddsfox.config.settings_polymarket",
    "oddsfox.config.settings",
)


def reload_all_settings_modules() -> ModuleType:
    """Reload settings submodules then the barrel; return the refreshed ``settings`` module."""
    out: ModuleType | None = None
    for name in _SETTINGS_CHAIN:
        out = importlib.reload(importlib.import_module(name))
    assert out is not None
    return out
