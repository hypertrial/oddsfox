import hashlib
import json

import httpx
import pytest

from oddsfox.http import (
    DOCUMENT_HOSTS,
    VENUE_HOSTS,
    is_public_ip,
    stream_get,
    validate_https_url,
)
from oddsfox.ir import fingerprint


def write_lineage(path, *, weights=b"weights", lineage="qwen2", architecture="qwen2"):
    revision = fingerprint([hashlib.sha256(weights).hexdigest()])
    path.with_name(f"{path.name}.oddsfox-lineage.json").write_text(
        json.dumps(
            {
                "schema": "oddsfox-model-lineage/1",
                "architecture": architecture,
                "lineage": lineage,
                "weights_revision": revision,
                "operator_reviewed": True,
            }
        )
    )


def test_rejects_loopback_and_mapped_addresses():
    assert is_public_ip("8.8.8.8")
    assert not is_public_ip("127.0.0.1")
    assert not is_public_ip("127.0.0.2")
    assert not is_public_ip("::1")
    assert not is_public_ip("::ffff:127.0.0.1")
    assert not is_public_ip("::ffff:127.0.0.2")
    assert not is_public_ip("10.0.0.1")
    assert not is_public_ip("169.254.1.1")
    assert not is_public_ip("fc00::1")
    assert is_public_ip("::ffff:8.8.8.8")
    assert not is_public_ip("64:ff9b::7f00:1")
    assert not is_public_ip("64:ff9b:1::7f00:1")
    assert is_public_ip("64:ff9b::808:808")


def test_stream_get_rejects_hosts_outside_the_allowlist():
    import inspect

    parameter = inspect.signature(stream_get).parameters["allowed_hosts"]
    assert parameter.default is inspect.Parameter.empty
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY

    def handler(request):
        raise AssertionError(f"non-allowlisted request reached transport: {request.url}")

    with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
        with pytest.raises(ValueError, match="approved official"):
            stream_get(
                client,
                "https://example.org/markets",
                allowed_hosts=VENUE_HOSTS,
                follow_redirects=False,
                max_bytes=1024,
            )


def test_venue_redirect_is_fail_closed():
    def handler(request):
        return httpx.Response(302, headers={"location": "https://gamma-api.polymarket.com/other"})

    with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
        with pytest.raises(ValueError, match="fail-closed"):
            stream_get(
                client,
                "https://gamma-api.polymarket.com/markets/x",
                allowed_hosts=VENUE_HOSTS,
                follow_redirects=False,
                max_bytes=1024,
            )


def test_document_redirect_requires_location():
    def handler(request):
        return httpx.Response(302)

    with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
        with pytest.raises(ValueError, match="Location"):
            stream_get(
                client,
                "https://docs.polymarket.com/rules",
                allowed_hosts=DOCUMENT_HOSTS,
                follow_redirects=True,
                max_bytes=1024,
            )


def test_document_redirect_revalidates_host(monkeypatch):
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if "docs.polymarket.com" in str(request.url):
            return httpx.Response(302, headers={"location": "https://evil.example/rules"})
        return httpx.Response(200, content=b"ok")

    with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
        with pytest.raises(ValueError, match="approved official"):
            stream_get(
                client,
                "https://docs.polymarket.com/rules",
                allowed_hosts=DOCUMENT_HOSTS,
                follow_redirects=True,
                max_bytes=1024,
            )


def test_document_redirect_rejects_http_downgrade():
    def handler(request):
        return httpx.Response(302, headers={"location": "http://docs.polymarket.com/rules"})

    with httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False) as client:
        with pytest.raises(ValueError, match="approved official"):
            stream_get(
                client,
                "https://docs.polymarket.com/rules",
                allowed_hosts=DOCUMENT_HOSTS,
                follow_redirects=True,
                max_bytes=1024,
            )


def test_mixed_public_and_private_dns_is_rejected(monkeypatch):
    monkeypatch.setattr(
        "socket.getaddrinfo",
        lambda *a, **k: [
            (None, None, None, None, ("8.8.8.8", 443)),
            (None, None, None, None, ("127.0.0.1", 443)),
        ],
    )
    from oddsfox.http import resolve_public

    with pytest.raises(ValueError, match="non-public"):
        resolve_public("docs.polymarket.com")


def test_mixed_public_and_mapped_loopback_dns_is_rejected(monkeypatch):
    monkeypatch.setattr(
        "socket.getaddrinfo",
        lambda *a, **k: [
            (None, None, None, None, ("8.8.8.8", 443)),
            (None, None, None, None, ("::ffff:127.0.0.1", 443, 0, 0)),
        ],
    )
    from oddsfox.http import resolve_public

    with pytest.raises(ValueError, match="non-public"):
        resolve_public("docs.polymarket.com")


def test_pinned_request_preserves_host_and_sni(monkeypatch):
    seen = {}

    class CaptureTransport(httpx.BaseTransport):
        def handle_request(self, request):
            seen["url"] = str(request.url)
            seen["host"] = request.headers["host"]
            seen["sni"] = request.extensions["sni_hostname"]
            return httpx.Response(200, content=b"ok", request=request)

    monkeypatch.setattr("oddsfox.http.resolve_public", lambda host: (host, "8.8.8.8"))
    with httpx.Client(transport=CaptureTransport()) as client:
        assert (
            stream_get(
                client,
                "https://gamma-api.polymarket.com/markets",
                allowed_hosts=VENUE_HOSTS,
                follow_redirects=False,
                max_bytes=1024,
            )
            == b"ok"
        )
    assert seen == {
        "url": "https://8.8.8.8/markets",
        "host": "gamma-api.polymarket.com",
        "sni": "gamma-api.polymarket.com",
    }


def test_validate_https_url_rejects_credentials_and_ports():
    with pytest.raises(ValueError):
        validate_https_url("http://docs.polymarket.com/x", DOCUMENT_HOSTS)
    with pytest.raises(ValueError):
        validate_https_url("https://user@docs.polymarket.com/x", DOCUMENT_HOSTS)
    assert (
        validate_https_url("https://docs.polymarket.com/x", DOCUMENT_HOSTS) == "docs.polymarket.com"
    )


def test_extractor_rss_kill(monkeypatch):
    from oddsfox.documents import supervise_extractor

    class Proc:
        pid = 9
        returncode = 1

        def poll(self):
            return None

        def kill(self):
            self.killed = True

        def communicate(self, timeout=5):
            return b"", b""

    monkeypatch.setattr("oddsfox.documents.sys.platform", "darwin")
    monkeypatch.setattr("oddsfox.documents.child_rss_bytes", lambda pid: 2 * 1024**3)
    proc = Proc()
    with pytest.raises(ValueError, match="memory budget"):
        supervise_extractor(proc, wall_seconds=5)
    assert proc.killed is True


def test_chat_template_pin(tmp_path, monkeypatch):
    from oddsfox.models import chat_template_text, model_manifest

    monkeypatch.setattr("oddsfox.models.importlib.metadata.version", lambda name: "0.test")
    path = tmp_path / "model"
    path.mkdir()
    (path / "config.json").write_text(
        json.dumps({"model_type": "qwen2", "quantization": {"bits": 4}})
    )
    (path / "tokenizer_config.json").write_text(json.dumps({"chat_template": "{{ bos }}"}))
    (path / "weights.safetensors").write_bytes(b"weights")
    write_lineage(path)
    text = chat_template_text(path)
    manifest = model_manifest(path)
    assert text == "{{ bos }}"
    assert manifest["family"] == "qwen2"
    assert manifest["chat_template_sha256"]
    (path / "tokenizer_config.json").write_text("{}")
    with pytest.raises(ValueError, match="chat template"):
        model_manifest(path)
    (path / "tokenizer_config.json").write_text(json.dumps({"chat_template": "{{ bos }}"}))
    write_lineage(path, architecture="unrelated")
    with pytest.raises(ValueError, match="does not match"):
        model_manifest(path)


def test_loaded_chat_template_must_match_pinned_manifest(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from oddsfox.models import model_manifest, verify_loaded_template

    monkeypatch.setattr("oddsfox.models.importlib.metadata.version", lambda name: "0.test")
    path = tmp_path / "model"
    path.mkdir()
    (path / "config.json").write_text(
        json.dumps({"model_type": "qwen2", "quantization": {"bits": 4}})
    )
    (path / "tokenizer_config.json").write_text(json.dumps({"chat_template": "{{ bos }}"}))
    (path / "weights.safetensors").write_bytes(b"weights")
    write_lineage(path)
    pinned = model_manifest(path)
    verify_loaded_template(SimpleNamespace(chat_template="{{ bos }}"), pinned)
    with pytest.raises(ValueError, match="does not match"):
        verify_loaded_template(SimpleNamespace(chat_template="{{ changed }}"), pinned)
    with pytest.raises(ValueError, match="no chat template"):
        verify_loaded_template(SimpleNamespace(chat_template=""), pinned)


def test_model_manifest_rejects_loaders_python_and_symlinks(tmp_path, monkeypatch):
    from oddsfox.models import model_manifest

    monkeypatch.setattr("oddsfox.models.importlib.metadata.version", lambda name: "0.test")
    path = tmp_path / "model"
    path.mkdir()
    (path / "tokenizer_config.json").write_text(json.dumps({"chat_template": "{{ bos }}"}))
    (path / "weights.safetensors").write_bytes(b"weights")
    (path / "config.json").write_text(
        json.dumps({"model_type": "qwen2", "quantization": {"bits": 4}, "model_file": "custom.py"})
    )
    with pytest.raises(ValueError, match="model_file"):
        model_manifest(path)
    (path / "config.json").write_text(
        json.dumps({"model_type": "qwen2", "quantization": {"bits": 4}})
    )
    (path / "hook.py").write_text("print('no')\n")
    with pytest.raises(ValueError, match="Python"):
        model_manifest(path)
    (path / "hook.py").unlink()
    (path / "hook.PY").write_text("print('no')\n")
    with pytest.raises(ValueError, match="Python"):
        model_manifest(path)
    (path / "hook.PY").unlink()
    (path / "escape").symlink_to("/etc/passwd")
    with pytest.raises(ValueError, match="symbolic links"):
        model_manifest(path)
