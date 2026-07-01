from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _nav_targets(items):
    for item in items:
        if isinstance(item, str):
            yield item
        elif isinstance(item, dict):
            for value in item.values():
                if isinstance(value, str):
                    yield value
                else:
                    yield from _nav_targets(value)


def test_mkdocs_nav_targets_exist():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())
    docs_dir = REPO_ROOT / "docs"

    for target in _nav_targets(config["nav"]):
        assert (docs_dir / target).is_file(), target


def test_mkdocs_uses_material_theme_contract():
    config = yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text())
    features = set(config["theme"]["features"])

    assert config["theme"]["name"] == "material"
    assert config["theme"]["logo"] == "assets/images/oddsfox-white.png"
    assert config["theme"]["favicon"] == "assets/images/oddsfox-white.png"
    assert config["extra_css"] == ["assets/stylesheets/oddsfox-dark.css"]
    assert config["plugins"] == ["search"]
    assert {
        "navigation.tabs",
        "navigation.sections",
        "navigation.expand",
        "navigation.indexes",
        "navigation.top",
        "toc.follow",
        "search.suggest",
        "search.highlight",
        "content.code.copy",
    } <= features


def test_readme_links_to_project_docs():
    readme = (REPO_ROOT / "README.md").read_text()

    assert "(docs/index.md)" in readme
    assert "(docs/quickstart.md)" in readme
    assert "(docs/architecture.md)" in readme
    assert "(docs/data-contracts.md)" in readme
    assert "(CONTRIBUTING.md)" in readme
    assert "(SECURITY.md)" in readme
    assert "(CHANGELOG.md)" in readme
    assert "(LICENSE)" in readme


def test_github_templates_exist():
    required = [
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/documentation.yml",
        ".github/PULL_REQUEST_TEMPLATE.md",
    ]

    for target in required:
        assert (REPO_ROOT / target).is_file(), target


def test_docs_make_targets_suppress_material_warning():
    makefile = (REPO_ROOT / "Makefile").read_text()

    assert "NO_MKDOCS_2_WARNING=true" in makefile


def test_docs_logo_asset_exists():
    logo = REPO_ROOT / "docs" / "assets" / "images" / "oddsfox-white.png"

    assert logo.is_file()
    assert logo.stat().st_size > 0


def test_built_docs_use_material_homepage():
    homepage = REPO_ROOT / "site" / "index.html"
    if not homepage.exists():
        pytest.skip("Run make docs-check before asserting generated HTML.")

    html = homepage.read_text()

    assert "md-header" in html
    assert "md-sidebar" in html
    assert "md-search" in html
    assert "Open-source prediction-market data pipeline" in html
    assert "oddsfox-dark.css" in html
    assert "assets/images/oddsfox-white.png" in html
    assert "Operator Manual" not in html
    assert "navbar-dark" not in html
    assert "bootstrap" not in html.lower()
