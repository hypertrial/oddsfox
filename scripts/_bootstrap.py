"""Shared repo-root and import-path setup for operator scripts."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

_REPO_ROOT: Path | None = None


def repo_root() -> Path:
    """Return the repository root (parent of ``scripts/``)."""
    global _REPO_ROOT
    if _REPO_ROOT is None:
        _REPO_ROOT = Path(__file__).resolve().parent.parent
    return _REPO_ROOT


def ensure_src_on_path() -> Path:
    """Insert ``src/`` on ``sys.path`` once; return the repository root."""
    root = repo_root()
    src = str(root / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    return root


REPO_ROOT: Final[Path] = repo_root()
