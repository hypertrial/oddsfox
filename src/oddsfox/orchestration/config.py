from __future__ import annotations

from typing import Literal

from dagster import Config
from pydantic import Field, field_validator, model_validator

from oddsfox.config.settings import (
    DEFAULT_ODDS_FIDELITY_MINUTES,
    MIN_ODDS_FIDELITY_MINUTES,
    WHALE_MIN_VOLUME_USD,
)

DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES = 20_000
DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES = 25
DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS = 60
DEFAULT_NO_PROGRESS_SOFT_TIMEOUT_SECONDS = 900
DEFAULT_NO_PROGRESS_HARD_TIMEOUT_SECONDS = 2700
DEFAULT_DBT_NO_PROGRESS_HARD_TIMEOUT_SECONDS = 3600
DEFAULT_PROGRESS_POLL_SECONDS = 5
_MARKET_SCOPE_VALUES = frozenset({"all", "wc2026"})


def _validate_market_scope_value(v: str) -> str:
    from oddsfox.ingestion.polymarket.wc2026_scope import validate_market_scope

    scope = validate_market_scope(v)
    if scope not in _MARKET_SCOPE_VALUES:
        raise ValueError(
            f"market_scope must be one of {sorted(_MARKET_SCOPE_VALUES)}, got {v!r}"
        )
    return scope


class GuardrailConfig(Config):
    progress_log_interval_seconds: int = Field(
        default=DEFAULT_PROGRESS_LOG_INTERVAL_SECONDS, ge=1
    )
    no_progress_soft_timeout_seconds: int | None = Field(
        default=DEFAULT_NO_PROGRESS_SOFT_TIMEOUT_SECONDS,
        ge=1,
    )
    no_progress_hard_timeout_seconds: int | None = Field(
        default=DEFAULT_NO_PROGRESS_HARD_TIMEOUT_SECONDS,
        ge=1,
    )
    progress_poll_seconds: int = Field(default=DEFAULT_PROGRESS_POLL_SECONDS, ge=1)
    raw_snapshot_level: str = Field(default="basic")

    @model_validator(mode="after")
    def _validate_soft_hard_timeouts(self) -> "GuardrailConfig":
        soft = self.no_progress_soft_timeout_seconds
        hard = self.no_progress_hard_timeout_seconds
        if soft is not None and hard is not None and hard <= soft:
            raise ValueError(
                "no_progress_hard_timeout_seconds must be greater than "
                "no_progress_soft_timeout_seconds when both are set"
            )
        return self

    @field_validator("raw_snapshot_level")
    @classmethod
    def _validate_raw_snapshot_level(cls, v: str) -> str:
        s = str(v).strip().lower()
        if s not in ("basic", "full"):
            raise ValueError("raw_snapshot_level must be 'basic' or 'full'")
        return s


class MarketsSyncConfig(GuardrailConfig):
    progress_log_interval_pages: int = Field(default=10, ge=1)
    discovery_mode: Literal["targeted", "full_keyset"] = "full_keyset"
    force_full_discovery: bool = False
    max_event_pages: int | None = None
    keyset_closed: bool | None = False
    keyset_tag_slugs: list[str] | None = None
    keyset_volume_min: float | None = Field(default=10000.0, ge=0)
    max_pages_without_progress: int | None = None

    @field_validator("max_pages_without_progress")
    @classmethod
    def _max_pages_without_progress_nonnegative(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_pages_without_progress must be >= 1 when set")
        return v


class Wc2026RegistryConfig(GuardrailConfig):
    max_event_pages: int | None = None
    keyset_closed: bool | None = False
    keyset_tag_slugs: list[str] | None = None
    keyset_volume_min: float | None = Field(default=10000.0, ge=0)
    max_pages_without_progress: int | None = None
    skip_if_snapshot_refreshed: bool = True
    force_refresh: bool = False

    @field_validator("max_pages_without_progress")
    @classmethod
    def _max_pages_without_progress_nonnegative(cls, v: int | None) -> int | None:
        if v is not None and v < 1:
            raise ValueError("max_pages_without_progress must be >= 1 when set")
        return v


class MetadataBackfillConfig(GuardrailConfig):
    batch_size: int = Field(default=50, ge=1, le=200)
    max_markets: int | None = None
    force: bool = False
    include_slugs: bool = True
    include_event_slugs: bool = True
    include_end_dates: bool = True
    gamma_requests_per_second: float | None = Field(default=None)
    event_slug_fallback_max_pages: int | None = Field(
        default=DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES
    )
    event_slug_fallback_max_pages_without_progress: int | None = Field(
        default=DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES
    )
    progress_log_interval_batches: int = Field(default=10, ge=1)
    event_slug_fallback_progress_pages: int = Field(default=25, ge=1)
    market_scope: str = Field(default="wc2026")

    @field_validator("gamma_requests_per_second")
    @classmethod
    def _gamma_rps_positive(cls, v: float | None) -> float | None:
        if v is not None and v <= 0:
            raise ValueError("gamma_requests_per_second must be positive when set")
        return v

    @field_validator("market_scope")
    @classmethod
    def _validate_market_scope(cls, v: str) -> str:
        return _validate_market_scope_value(v)


class OddsSyncConfig(GuardrailConfig):
    workers: int = 40
    batch_size: int = 50_000
    progress_log_interval_tokens: int = Field(default=100, ge=1)
    fidelity: int = Field(
        default=DEFAULT_ODDS_FIDELITY_MINUTES, ge=MIN_ODDS_FIDELITY_MINUTES
    )
    requests_per_second: int | None = 40
    auto_tune_rps: bool = True
    auto_tune_max_rps: int | None = Field(default=60, ge=1)
    force: bool = False
    clob_cutoff: str = "2023-01-01"
    skip_recent_minutes: int = 15
    overlap_minutes: int = 5
    window_hours: int = 8760
    rebuild_minutely: bool = False
    reconcile_ledger: bool = False
    empty_skip_runs: int = 2
    routine_interval_hours: int = Field(default=6, ge=1)
    empty_retry_base_hours: int = Field(default=24, ge=1)
    empty_retry_max_hours: int = Field(default=168, ge=1)
    error_retry_minutes: int = Field(default=30, ge=1)
    transient_retries: int = 2
    transient_backoff_seconds: float = 0.25
    short_range_first: bool = True
    market_page_size: int = 2000
    market_scope: str = Field(default="wc2026")
    ended_market_grace_days: int | None = Field(default=7, ge=0)
    min_volume: float | None = None
    max_volume: float | None = Field(default=WHALE_MIN_VOLUME_USD)
    minutely_backfill_days: int = Field(default=0, ge=0)

    @field_validator("market_scope")
    @classmethod
    def _validate_market_scope(cls, v: str) -> str:
        return _validate_market_scope_value(v)

    @field_validator("min_volume", "max_volume")
    @classmethod
    def _validate_volume_bound(cls, v: float | None) -> float | None:
        if v is None:
            return None
        parsed = float(v)
        if parsed < 0:
            raise ValueError("volume bounds must be >= 0")
        return parsed


class MinutelyOddsSyncConfig(OddsSyncConfig):
    fidelity: int = Field(default=1, ge=MIN_ODDS_FIDELITY_MINUTES)
    force: bool = True
    skip_recent_minutes: int = 1
    overlap_minutes: int = 2
    window_hours: int = 12
    routine_interval_hours: int = Field(default=1, ge=1)
    min_volume: float | None = Field(default=WHALE_MIN_VOLUME_USD)
    max_volume: float | None = None
    minutely_backfill_days: int = Field(default=0, ge=0)
    market_scope: str = Field(default="wc2026")
    ended_market_grace_days: int | None = Field(default=7, ge=0)


class RepairConfig(Config):
    persist_run_metrics: bool = True
    raw_snapshot_level: str = Field(default="basic")


class DbtBuildConfig(GuardrailConfig):
    progress_log_interval_events: int = Field(default=20, ge=1)
    no_progress_hard_timeout_seconds: int | None = Field(
        default=DEFAULT_DBT_NO_PROGRESS_HARD_TIMEOUT_SECONDS,
        ge=1,
    )
    full_refresh: bool = False


def full_refresh_events_run_config() -> dict:
    markets_cfg = MarketsSyncConfig(
        discovery_mode="full_keyset",
        force_full_discovery=True,
        max_pages_without_progress=None,
    )
    registry_cfg = Wc2026RegistryConfig(
        force_refresh=True,
        max_pages_without_progress=None,
    )
    return {
        "ops": {
            "polymarket_markets_snapshot": {"config": markets_cfg.model_dump()},
            "polymarket_wc2026_registry": {"config": registry_cfg.model_dump()},
        }
    }


def dbt_full_refresh_run_config() -> dict:
    dbt_cfg = DbtBuildConfig(full_refresh=True)
    return {"ops": {"polymarket_dbt": {"config": dbt_cfg.model_dump()}}}


def minutely_odds_run_config() -> dict:
    odds_cfg = MinutelyOddsSyncConfig(force=True, overlap_minutes=1)
    return {
        "ops": {
            "polymarket_token_odds_history_minutely": {"config": odds_cfg.model_dump()},
        }
    }


def minutely_odds_cold_run_config() -> dict:
    odds_cfg = MinutelyOddsSyncConfig(force=False, overlap_minutes=2)
    return {
        "ops": {
            "polymarket_token_odds_history_minutely": {"config": odds_cfg.model_dump()},
        }
    }
