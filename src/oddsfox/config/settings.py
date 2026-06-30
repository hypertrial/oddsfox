"""Central settings barrel for the v0.1.0 WC2026 Polymarket pipeline."""

from __future__ import annotations

import oddsfox.config.settings_polymarket as _settings_polymarket
import oddsfox.config.settings_warehouse as _settings_warehouse
from oddsfox.config._env import (  # noqa: F401
    _env_bool,
    _env_date,
    _env_float,
    _env_int,
    _optional_env_float,
    _optional_env_int,
    _optional_env_str,
)
from oddsfox.config.settings_polymarket import *  # noqa: F403
from oddsfox.config.settings_warehouse import *  # noqa: F403

__all__ = list(
    dict.fromkeys(
        (
            "_env_bool",
            "_env_date",
            "_env_float",
            "_env_int",
            "_optional_env_float",
            "_optional_env_int",
            "_optional_env_str",
            *_settings_warehouse.__all__,
            *_settings_polymarket.__all__,
        )
    )
)
