import json

import httpx
import pytest
from fastapi.testclient import TestClient

from oddsfox.app import create_app
from oddsfox.demo import load_demo
from oddsfox.ingest import fetch, import_capture, normalize


def test_loopback_host_origin_and_mutation_token(store):
    with TestClient(
        create_app(store, auto_sync=False, token="test-session"), base_url="http://127.0.0.1:8777"
    ) as client:
        assert client.get("/").status_code == 200
        assert client.get("/api/status", headers={"Host": "evil.example"}).status_code == 400
        assert (
            client.get("/api/status", headers={"Origin": "https://evil.example"}).status_code == 403
        )
        assert client.post("/api/publish", json={}).status_code == 403
        assert (
            client.post(
                "/api/publish",
                json={},
                headers={"X-Oddsfox-Token": "test-session", "Origin": "https://evil.example"},
            ).status_code
            == 403
        )
        assert client.post(
            "/api/publish", json={}, headers={"X-Oddsfox-Token": "test-session"}
        ).json() == {"assertions": []}
        assert client.get("/").headers["Content-Security-Policy"].startswith("default-src 'self'")
        assert "test-session" not in client.get("/").text


def test_report_read_does_not_approve_and_explicit_reviews_publish(store):
    result = load_demo(store)
    headers = {"X-Oddsfox-Token": "test"}
    with TestClient(
        create_app(store, auto_sync=False, token="test"), base_url="http://127.0.0.1:8777"
    ) as client:
        assert len(client.get("/api/report").json()["comparisons"]) == 2
        assert client.get("/api/assertions").json() == []
        assert client.get("/api/export").json()["assertions"] == []
        for identity in result["interpretations"]:
            response = client.post(
                f"/api/reviews/{identity}",
                json={
                    "reviewer": "Test reviewer",
                    "rationale": "checked evidence",
                    "approve": True,
                    "governing_material_complete": True,
                },
                headers=headers,
            )
            assert response.status_code == 200, response.text
        assert len(client.post("/api/publish", json={}, headers=headers).json()["assertions"]) == 2
        assert len(client.get("/api/assertions").json()) == 2
        assert client.get("/assets/../../app.py").status_code == 404


def test_brand_assets_are_allowlisted_with_explicit_types(store):
    expected = {
        "app.js": "text/javascript",
        "events.js": "text/javascript",
        "style.css": "text/css",
        "logo.png": "image/png",
        "inter.woff2": "font/woff2",
        "jetbrains-mono.woff2": "font/woff2",
    }
    with TestClient(
        create_app(store, auto_sync=False, token="secret-session"),
        base_url="http://127.0.0.1:8777",
    ) as client:
        for name, media_type in expected.items():
            response = client.get(f"/assets/{name}")
            assert response.status_code == 200, name
            assert response.headers["content-type"].startswith(media_type)
            assert len(response.content) > 32
            if media_type.startswith(("image/", "font/")):
                assert "charset" not in response.headers["content-type"]
        assert client.get("/assets/logo.png").content.startswith(b"\x89PNG\r\n\x1a\n")
        assert client.get("/assets/inter.woff2").content.startswith(b"wOF2")
        assert client.get("/assets/jetbrains-mono.woff2").content.startswith(b"wOF2")
        assert client.get("/assets/missing.css").status_code == 404
        assert client.get("/assets/OFL-Inter.txt").status_code == 404
        for blocked in (
            "OFL-JetBrainsMono.txt",
            "index.html",
            "research.html",
            "Logo.png",
            "INTER.woff2",
            "app.py",
            "fonts/inter.woff2",
            "fonts/jetbrains-mono.woff2",
        ):
            assert client.get(f"/assets/{blocked}").status_code == 404, blocked
        home_response = client.get("/")
        research_response = client.get("/research")
        home = home_response.text
        research = research_response.text
        style = client.get("/assets/style.css").text
        csp = home_response.headers["Content-Security-Policy"]
        assert csp.startswith("default-src 'self'")
        for directive in (
            "script-src 'self'",
            "style-src 'self'",
            "img-src 'self'",
            "font-src 'self'",
        ):
            assert directive in csp
        assert "unsafe-inline" not in csp
        assert "https:" not in csp
        for page in (home, research):
            assert "/assets/logo.png" in page
            assert 'class="skip"' in page
            assert 'href="#main-content"' in page
            assert 'id="main-content"' in page
            assert 'id="main-content" tabindex="-1"' in page
            assert "Skip to content" in page
            assert "secret-session" not in page
            assert "fonts.googleapis.com" not in page
            assert "fonts.gstatic.com" not in page
            assert "innerHTML" not in page
        for identity in (
            "sync",
            "pause",
            "notice",
            "metrics",
            "model-status",
            "token",
            "sync-status",
            "filter-venue",
            "filter-category",
            "filter-qualification",
            "filter-analysis",
            "page-label",
            "event-list",
            "previous",
            "next",
            "event-detail",
        ):
            assert f'id="{identity}"' in home, identity
        for identity in (
            "refresh",
            "notice",
            "metrics",
            "token",
            "reviewer",
            "venue",
            "ids",
            "capture",
            "payload",
            "import",
            "contract-list",
            "publish",
            "comparison-list",
            "activity-list",
            "registry-name",
            "registry-json",
            "registry-rationale",
            "register",
        ):
            assert f'id="{identity}"' in research, identity
        for label in (
            ">Refresh now</button>",
            ">Pause sync</button>",
            ">Previous</button>",
            ">Next</button>",
        ):
            assert label in home
        assert 'class="secondary"' in home
        assert 'class="header-inner"' in home
        assert 'class="brand-kicker"' in home
        assert 'class="header-inner"' in research
        for label in (
            ">Refresh report</button>",
            ">Capture contracts</button>",
            ">Import captured payload</button>",
            ">Publish reviewed claims</button>",
            ">Record reviewed definition</button>",
        ):
            assert label in research
        assert '<section id="event-detail" class="panel" hidden' in home
        assert "<textarea" in research and 'id="payload"' in research
        assert 'id="registry-json"' in research
        assert "<details" in home and "<details" in research
        assert 'src="/assets/events.js"' in home
        assert 'src="/assets/app.js"' in research
        assert "@font-face" in style
        assert "/assets/inter.woff2" in style
        assert "/assets/jetbrains-mono.woff2" in style
        for token in (
            "#0b1120",
            "#151e32",
            "#1e293b",
            "#334155",
            "#f1f5f9",
            "#94a3b8",
            "#7b8da8",
            "#ff5722",
            "#00e5ff",
            "#10b981",
            "#f87171",
            "#64748b",
            "#f59e0b",
        ):
            assert token in style, token
        assert "color-scheme: dark" in style
        assert "[hidden]" in style
        assert "none !important" in style
        assert "prefers-reduced-motion" in style
        assert ":focus-visible" in style
        assert "button.secondary" in style
        assert "button.danger" in style
        assert "clip-path" in style
        assert "::-webkit-details-marker" in style
        assert "prefers-color-scheme: light" not in style
        assert "fonts.googleapis.com" not in style
        assert "https://" not in style
        assert "innerHTML" not in client.get("/assets/events.js").text
        assert "innerHTML" not in client.get("/assets/app.js").text


def test_captured_markup_is_data_and_missing_documents_explicit(store):
    raw = json.dumps(
        {
            "id": "a",
            "question": "<img src=x onerror=alert(1)>",
            "description": "Rules at https://example.org/rules",
            "outcomes": '["yes","no"]',
        }
    ).encode()
    identity = import_capture(store, "polymarket", raw)
    record = store.get(identity)
    assert store.artifact(record["data"]["payload_artifact"]) == raw
    assert record["data"]["references"][0]["status"] == "not_captured"
    with TestClient(
        create_app(store, auto_sync=False, token="x"), base_url="http://127.0.0.1:8777"
    ) as client:
        assert "onerror" not in client.get("/").text
        assert "textContent" in client.get("/assets/app.js").text
        assert "innerHTML" not in client.get("/assets/app.js").text


def test_adapter_capture_and_refresh_failure_are_observable(store):
    def handler(request):
        if request.url.path.endswith("bad"):
            return httpx.Response(404, json={"detail": "missing"})
        return httpx.Response(
            200,
            json={
                "market": {
                    "ticker": "BTC-YES",
                    "market_type": "binary",
                    "title": "BTC threshold",
                    "rules_primary": "Exact primary rules",
                    "rules_secondary": "Exceptions",
                }
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = fetch(store, "kalshi", ["BTC-YES", "bad"], client)
    assert [r["state"] for r in result] == ["captured", "failed"]
    assert any(not r["success"] for r in store.status()["refreshes"])
    record = store.get(result[0]["version_id"])
    assert record["data"]["metadata"]["observation_date"] is None
    assert set(record["data"]["text_artifacts"]) == {
        "title",
        "rules_primary",
        "rules_secondary",
        "structured_contract_fields",
    }


@pytest.mark.parametrize("raw", [b"[]", b'{"id":"a","id":"b"}', b'{"id":"a","outcomes":{}}'])
def test_malformed_capture_rejected(raw):
    with pytest.raises(ValueError):
        normalize("polymarket", raw)


def test_malformed_and_oversize_api_input(store):
    with TestClient(
        create_app(store, auto_sync=False, token="t"), base_url="http://127.0.0.1:8777"
    ) as client:
        assert (
            client.post(
                "/api/interpret", json={"ir": {}}, headers={"X-Oddsfox-Token": "t"}
            ).status_code
            == 422
        )
        response = client.post(
            "/api/import", content=b"x" * (4 * 1024 * 1024 + 1), headers={"X-Oddsfox-Token": "t"}
        )
        assert response.status_code == 413


def test_real_style_decimal_strike_ticker_is_allowed(store):
    ticker = "KXBTCD-26SEP0806-T88299.99"
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "market": {
                        "ticker": ticker,
                        "market_type": "binary",
                        "rules_primary": "single threshold",
                    }
                },
            )
        )
    ) as client:
        result = fetch(store, "kalshi", [ticker], client)
    assert result[0]["state"] == "captured"


@pytest.mark.parametrize(
    "content_type", ["application/json", "Application/JSON", "application/problem+json"]
)
def test_duplicate_keys_rejected_for_all_json_media_types(store, content_type):
    with TestClient(
        create_app(store, auto_sync=False, token="t"), base_url="http://127.0.0.1:8777"
    ) as client:
        response = client.post(
            "/api/import",
            content=b'{"platform":"kalshi","platform":"polymarket","payload":"{}"}',
            headers={"Content-Type": content_type, "X-Oddsfox-Token": "t"},
        )
        assert response.status_code == 422
        assert "duplicate" in response.json()["detail"]


@pytest.mark.parametrize("malformed", [None, [], "invalid", 42])
def test_malformed_market_records_failure_and_continues(store, malformed):
    def response(request):
        ticker = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(
            200,
            json={"market": {"ticker": ticker, "market_type": "binary", "rules_primary": "rules"}},
        )

    with httpx.Client(transport=httpx.MockTransport(response)) as client:
        fetch(store, "kalshi", ["A"], client)

    def failing_response(request):
        return (
            httpx.Response(200, json={"market": malformed})
            if request.url.path.endswith("/A")
            else response(request)
        )

    with httpx.Client(transport=httpx.MockTransport(failing_response)) as client:
        result = fetch(store, "kalshi", ["A", "B"], client)
    assert [r["state"] for r in result] == ["failed", "captured"]
    refreshes = [r for r in store.status()["refreshes"] if r["logical"] == "kalshi:A"]
    assert refreshes[0]["success"] is False
    assert refreshes[0]["version_id"] == refreshes[1]["version_id"]
