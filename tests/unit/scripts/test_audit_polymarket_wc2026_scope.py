"""Tests for scripts/audit_polymarket_wc2026_scope.py."""

from __future__ import annotations

import importlib
import sys
from contextlib import contextmanager
from types import SimpleNamespace


def _load_audit_module():
    scripts_dir = __file__.rsplit("/tests/", 1)[0] + "/scripts"
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    return importlib.import_module("audit_polymarket_wc2026_scope")


class _Result:
    def __init__(self, *, one=None, rows=()):
        self._one = one
        self._rows = rows

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._rows


class _AuditConn:
    def __init__(self, *, total=5, strict=3, gaps=0, by_source=None):
        self.total = total
        self.strict = strict
        self.gaps = gaps
        self.by_source = by_source or {"allowlist": 3}

    def execute(self, sql):
        if "GROUP BY source" in sql:
            return _Result(rows=[(k, v) for k, v in self.by_source.items()])
        if "lower(coalesce" in sql:
            return _Result(one=(self.gaps,))
        if "WHERE STRICT_SCOPE" in sql:
            return _Result(one=(self.strict,))
        return _Result(one=(self.total,))


def _patch_runtime(monkeypatch, audit, conn):
    @contextmanager
    def connection():
        yield conn

    monkeypatch.setattr(audit, "ensure_duck_db", lambda: None)
    monkeypatch.setattr(audit, "get_connection", connection)
    monkeypatch.setattr(audit, "registry_market_count", lambda: 3)
    monkeypatch.setattr(audit, "polymarket_raw_tbl", lambda name: f"raw.{name}")
    monkeypatch.setattr(audit, "polymarket_ops_tbl", lambda name: f"ops.{name}")
    monkeypatch.setattr(
        audit, "market_scope_predicate_sql", lambda *_args: "STRICT_SCOPE"
    )
    monkeypatch.setattr(
        audit,
        "load_wc2026_config",
        lambda: SimpleNamespace(
            event_slugs=("2026-fifa-world-cup-winner",),
            event_slug_prefixes=("2026-fifa-world-cup",),
        ),
    )


def test_audit_wc2026_scope_prints_summary(monkeypatch, capsys):
    audit = _load_audit_module()
    _patch_runtime(monkeypatch, audit, _AuditConn())
    monkeypatch.setattr(sys, "argv", ["audit_polymarket_wc2026_scope.py"])

    assert audit.main() == 0

    out = capsys.readouterr().out
    assert "Markets total: 5" in out
    assert "Registry rows: 3" in out
    assert "Strict wc2026 markets: 3" in out
    assert "Allowlisted event_slug not strict-scoped: 0" in out


def test_audit_wc2026_scope_can_fail_on_allowlist_gaps(monkeypatch):
    audit = _load_audit_module()
    _patch_runtime(monkeypatch, audit, _AuditConn(gaps=1))
    monkeypatch.setattr(
        sys,
        "argv",
        ["audit_polymarket_wc2026_scope.py", "--fail-on-allowlist-gaps"],
    )

    assert audit.main() == 1


def test_audit_wc2026_scope_can_fail_on_discovery_rows(monkeypatch):
    audit = _load_audit_module()
    _patch_runtime(monkeypatch, audit, _AuditConn(by_source={"discovery": 2}))
    monkeypatch.setattr(
        sys,
        "argv",
        ["audit_polymarket_wc2026_scope.py", "--fail-on-discovery-rows"],
    )

    assert audit.main() == 1
