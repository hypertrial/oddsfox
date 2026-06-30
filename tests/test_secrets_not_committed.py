"""Test to ensure secrets are not committed to the repository."""

import re
import subprocess
from pathlib import Path

import pytest
from dotenv import dotenv_values

pytestmark = pytest.mark.repo_check

REPO_ROOT = Path(__file__).resolve().parent.parent

STATIC_SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "pem_private_key",
        re.compile(r"BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY"),
    ),
    (
        "clob_env_assignment",
        re.compile(
            r"(?:CLOB_API_KEY|CLOB_API_SECRET|CLOB_API_PASSPHRASE)\s*[:=]\s*"
            r"['\"]?[^\s'\"#]{12,}"
        ),
    ),
)


def load_secrets_from_env() -> list[str]:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return []

    env_values = dotenv_values(env_path)
    secrets = []
    for key, value in env_values.items():
        if not value:
            continue
        if "PROXY" in key.upper() or "proxy" in key.lower():
            for proxy_url in value.split(","):
                proxy_url = proxy_url.strip()
                if "@" in proxy_url:
                    match = re.search(r"://([^:]+):([^@]+)@", proxy_url)
                    if match:
                        username = match.group(1)
                        password = match.group(2)
                        if len(username) > 5 and username not in ("user", "username"):
                            secrets.append(username.split("__")[0])
                        if len(password) > 5 and password not in ("pass", "password"):
                            secrets.append(password)
        elif any(marker in key.upper() for marker in ("API_KEY", "SECRET", "TOKEN")):
            if len(value) > 10 and value not in ("your_api_key_here", "placeholder"):
                secrets.append(value.strip("'\""))
    return list(set(secrets))


ENV_SECRET_PATTERNS = load_secrets_from_env()
ALLOWED_FILES = {".env", ".env.example", "tests/test_secrets_not_committed.py"}
SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    ".mypy_cache",
    "*.egg-info",
}


def get_tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True
    )
    if result.returncode != 0:
        pytest.fail("Not a git repository or git not available")
    return result.stdout.strip().split("\n")


def should_skip_file(filepath: str) -> bool:
    path = Path(filepath)
    if path.name in ALLOWED_FILES:
        return True
    return any(part in SKIP_DIRS for part in path.parts)


def is_binary_file(filepath: Path) -> bool:
    try:
        with open(filepath, "rb") as f:
            return b"\x00" in f.read(8192)
    except OSError:
        return True


def _scan_tracked_files(*, patterns: list[tuple[str, str]]) -> list[str]:
    violations: list[str] = []
    for filepath in get_tracked_files():
        if should_skip_file(filepath):
            continue
        full_path = REPO_ROOT / filepath
        if (
            not full_path.exists()
            or not full_path.is_file()
            or is_binary_file(full_path)
        ):
            continue
        content = full_path.read_text(encoding="utf-8", errors="ignore")
        for label, pattern in patterns:
            if isinstance(pattern, re.Pattern):
                if pattern.search(content):
                    violations.append(f"{filepath}: matched static pattern {label}")
            elif pattern in content:
                violations.append(f"{filepath}: contains '{pattern[:10]}...'")
    return violations


def test_static_secret_patterns_are_non_empty() -> None:
    assert STATIC_SECRET_PATTERNS


def test_no_static_secrets_in_tracked_text_files() -> None:
    violations = _scan_tracked_files(
        patterns=[(label, regex) for label, regex in STATIC_SECRET_PATTERNS]
    )
    assert not violations, (
        "Static secret patterns matched tracked files:\n" + "\n".join(violations)
    )


def test_no_secrets_in_tracked_text_files():
    violations = _scan_tracked_files(
        patterns=[("env_secret", secret) for secret in ENV_SECRET_PATTERNS]
    )
    assert not violations, "Secrets found in tracked files:\n" + "\n".join(violations)


def test_env_file_is_gitignored_and_not_tracked():
    gitignore = (REPO_ROOT / ".gitignore").read_text()
    assert any(pattern in gitignore for pattern in (".env", "*.env", ".env*"))
    tracked_files = get_tracked_files()
    env_files = [f for f in tracked_files if f == ".env" or f.endswith("/.env")]
    assert not env_files
