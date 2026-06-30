"""Tests for scripts/_bootstrap.py."""

from __future__ import annotations

import sys
from pathlib import Path


def test_ensure_src_on_path_is_idempotent() -> None:
    scripts_dir = Path(__file__).resolve().parents[2] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    from _bootstrap import ensure_src_on_path, repo_root

    root = ensure_src_on_path()
    src = str(root / "src")
    assert (root / "src" / "oddsfox").is_dir()
    assert src in sys.path
    again = ensure_src_on_path()
    assert again == root
    assert sys.path.count(src) == 1
    assert repo_root() == root
