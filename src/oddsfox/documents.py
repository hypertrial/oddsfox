"""Bounded official governing documents; redirects cannot widen the trust boundary."""

import ipaddress
import json
import socket
import subprocess
import sys
import tempfile
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx

MAX_BYTES = 8 * 1024 * 1024
MAX_TEXT = 2 * 1024 * 1024
OFFICIAL_HOSTS = frozenset(
    {
        "kalshi.com",
        "www.kalshi.com",
        "kalshi-public-docs.s3.amazonaws.com",
        "kalshi-public-docs.s3.us-east-1.amazonaws.com",
        "kalshi-public-docs.s3.us-east-2.amazonaws.com",
        "polymarket.com",
        "www.polymarket.com",
        "docs.polymarket.com",
        "polymarket.us",
        "www.polymarket.us",
        "docs.polymarket.us",
        "polymarketexchange.com",
        "www.polymarketexchange.com",
    }
)


class TextParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style"}:
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in {"script", "style"}:
            self.hidden = max(0, self.hidden - 1)
        if tag in {"p", "div", "br", "li", "h1", "h2", "h3", "tr"}:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def extract(raw: bytes) -> str:
    if raw.startswith(b"%PDF-"):
        from io import BytesIO

        from pypdf import PdfReader

        reader = PdfReader(BytesIO(raw))
        if reader.is_encrypted or len(reader.pages) > 200:
            raise ValueError("encrypted PDF or more than 200 pages")
        parts = []
        size = 0
        for number, page in enumerate(reader.pages):
            content = page.extract_text() or ""
            if not content.strip():
                raise ValueError(
                    "PDF page has no extractable text; governing material is incomplete"
                )
            text = f"\nPage {number + 1}\n" + content
            size += len(text.encode())
            if size > MAX_TEXT:
                raise ValueError("document exceeds extracted-text limit")
            parts.append(text)
        text = "".join(parts)
    else:
        parser = TextParser()
        parser.feed(raw.decode("utf-8", errors="replace"))
        text = "".join(parser.parts).strip()
    if not text or len(text.encode()) > MAX_TEXT:
        raise ValueError("empty document or extracted text exceeds 2 MiB")
    return text


def validate_url(url: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in OFFICIAL_HOSTS
        or parsed.username
        or parsed.password
        or parsed.port not in {None, 443}
    ):
        raise ValueError("document host is not an approved official HTTPS host")
    addresses = {
        str(a[4][0]) for a in socket.getaddrinfo(parsed.hostname, 443, type=socket.SOCK_STREAM)
    }
    if not addresses or any(not ipaddress.ip_address(a).is_global for a in addresses):
        raise ValueError("document host resolves to a non-public network")
    return parsed.hostname, sorted(addresses)[0]


def retrieve(url: str) -> bytes:
    # Pin the validated IP while retaining Host and TLS SNI/certificate checks.
    with httpx.Client(timeout=20, trust_env=False, follow_redirects=False) as client:
        for _ in range(4):
            host, address = validate_url(url)
            target = httpx.URL(url).copy_with(host=address)
            with client.stream(
                "GET", target, headers={"Host": host}, extensions={"sni_hostname": host}
            ) as response:
                if response.is_redirect:
                    url = urljoin(url, response.headers["location"])
                    continue
                response.raise_for_status()
                chunks, size = [], 0
                import time

                started = time.monotonic()
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > MAX_BYTES or time.monotonic() - started > 45:
                        raise ValueError("document download exceeds resource limit")
                    chunks.append(chunk)
                raw = b"".join(chunks)
                break
        else:
            raise ValueError("too many document redirects")
    return raw


def extract_bounded(raw: bytes) -> str:
    # Parsing runs in a disposable process: malformed PDFs cannot monopolize the worker.
    with tempfile.TemporaryDirectory(prefix="oddsfox-document-") as directory:
        path = Path(directory) / "source"
        path.write_bytes(raw)
        result = subprocess.run(
            [sys.executable, "-m", "oddsfox.documents", str(path)],
            capture_output=True,
            timeout=60,
            check=False,
        )
        if result.returncode or len(result.stdout) > MAX_TEXT * 6:
            raise ValueError("governing document could not be safely extracted")
        return json.loads(result.stdout)


def capture_document(store, url: str) -> dict:
    """Failures are evidence too; never silently replace complete material with a guess."""
    artifact = None
    try:
        raw = retrieve(url)
        artifact = store.put_artifact(raw)
        text = extract_bounded(raw)
        return {
            "url": url,
            "status": "captured",
            "text": text,
            "raw_artifact": artifact,
        }
    except (ValueError, OSError, httpx.HTTPError, subprocess.SubprocessError) as exc:
        return {
            "url": url,
            "status": "inaccessible",
            "text": "",
            "raw_artifact": artifact,
            "reason": str(exc)[:500],
        }


if __name__ == "__main__":
    import resource

    resource.setrlimit(resource.RLIMIT_CPU, (45, 45))
    # RLIMIT_AS is not portable on macOS; input/page/output limits still apply there.
    if sys.platform == "linux":
        resource.setrlimit(resource.RLIMIT_AS, (1024**3, 1024**3))
    print(json.dumps(extract(Path(sys.argv[1]).read_bytes())))
