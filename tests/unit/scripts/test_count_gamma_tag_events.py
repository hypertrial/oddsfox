"""Tests for scripts/count_gamma_tag_events.py."""

from __future__ import annotations

import argparse
import importlib
import sys
from types import SimpleNamespace

import pytest


def _load_count_module():
    scripts_dir = __file__.rsplit("/tests/", 1)[0] + "/scripts"
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    return importlib.import_module("count_gamma_tag_events")


def test_parse_keyset_closed_values():
    count = _load_count_module()

    assert count._parse_keyset_closed("open") is False
    assert count._parse_keyset_closed("closed") is True
    assert count._parse_keyset_closed("any") is None
    with pytest.raises(argparse.ArgumentTypeError):
        count._parse_keyset_closed("bad")


def test_count_tag_events_uses_mocked_keyset_pages(monkeypatch):
    count = _load_count_module()
    client = object()
    calls = []

    def iter_events(client_arg, **kwargs):
        calls.append((client_arg, kwargs))
        yield (
            [{"id": "e1"}, {"id": "e2"}],
            SimpleNamespace(pages_done=1, truncated=False),
        )
        yield [{"id": "e3"}], SimpleNamespace(pages_done=2, truncated=False)
        yield [], SimpleNamespace(pages_done=3, truncated=False)

    monkeypatch.setattr(count, "build_client", lambda: client)
    monkeypatch.setattr(count, "iter_gamma_events_keyset", iter_events)

    assert (
        count.count_tag_events(
            "fifa-world-cup",
            keyset_closed=False,
            keyset_volume_min=0.0,
            log_every=2,
            max_pages=5,
        )
        == 3
    )
    assert calls == [
        (
            client,
            {
                "max_pages": 5,
                "keyset_tag_slug": "fifa-world-cup",
                "keyset_closed": False,
                "keyset_volume_min": 0.0,
            },
        )
    ]


def test_count_main_resolves_tags_and_options(monkeypatch):
    count = _load_count_module()
    captured = {}
    config = object()

    def resolve(tags, *, config):
        captured["resolve"] = (tags, config)
        return ["fifa-world-cup"]

    def count_scope_tags(tag_slugs, **kwargs):
        captured["count"] = (tag_slugs, kwargs)
        return {"fifa-world-cup": 2}

    monkeypatch.setattr(count, "load_wc2026_config", lambda: config)
    monkeypatch.setattr(count, "resolve_keyset_tag_slugs", resolve)
    monkeypatch.setattr(count, "count_scope_tags", count_scope_tags)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "count_gamma_tag_events.py",
            "--tag",
            "fifa-world-cup",
            "--keyset-closed",
            "any",
            "--keyset-volume-min",
            "0",
            "--log-every",
            "5",
            "--max-pages",
            "2",
        ],
    )

    assert count.main() == 0
    assert captured["resolve"] == (["fifa-world-cup"], config)
    assert captured["count"] == (
        ["fifa-world-cup"],
        {
            "keyset_closed": None,
            "keyset_volume_min": 0.0,
            "log_every": 5,
            "max_pages": 2,
        },
    )


@pytest.mark.parametrize(
    "args",
    [
        ["--log-every", "0"],
        ["--keyset-volume-min", "-1"],
    ],
)
def test_count_main_rejects_invalid_inputs(monkeypatch, args):
    count = _load_count_module()
    monkeypatch.setattr(count, "load_wc2026_config", lambda: object())
    monkeypatch.setattr(
        count, "resolve_keyset_tag_slugs", lambda *_args, **_kwargs: ["tag"]
    )
    monkeypatch.setattr(sys, "argv", ["count_gamma_tag_events.py", *args])

    with pytest.raises(SystemExit) as exc:
        count.main()

    assert exc.value.code == 2


def test_count_main_rejects_empty_tag_resolution(monkeypatch):
    count = _load_count_module()
    monkeypatch.setattr(count, "load_wc2026_config", lambda: object())
    monkeypatch.setattr(count, "resolve_keyset_tag_slugs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(sys, "argv", ["count_gamma_tag_events.py"])

    with pytest.raises(SystemExit) as exc:
        count.main()

    assert exc.value.code == 2
