import json

import httpx
import pytest
from fastapi.testclient import TestClient

from oddsfox.app import create_app
from oddsfox.demo import load_demo
from oddsfox.ingest import fetch, import_capture, normalize


def test_loopback_host_origin_and_mutation_token(store):
    with TestClient(
        create_app(store, token="test-session"), base_url="http://127.0.0.1:8777"
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
    with TestClient(create_app(store, token="test"), base_url="http://127.0.0.1:8777") as client:
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
    with TestClient(create_app(store, token="x"), base_url="http://127.0.0.1:8777") as client:
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
    assert set(record["data"]["text_artifacts"]) == {"title", "rules_primary", "rules_secondary"}


@pytest.mark.parametrize("raw", [b"[]", b'{"id":"a","id":"b"}', b'{"id":"a","outcomes":{}}'])
def test_malformed_capture_rejected(raw):
    with pytest.raises(ValueError):
        normalize("polymarket", raw)


def test_malformed_and_oversize_api_input(store):
    with TestClient(create_app(store, token="t"), base_url="http://127.0.0.1:8777") as client:
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
    with TestClient(create_app(store, token="t"), base_url="http://127.0.0.1:8777") as client:
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
