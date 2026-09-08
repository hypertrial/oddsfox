"""Bounded official governing documents; redirects cannot widen the trust boundary."""

import json
import subprocess
import sys
import tempfile
import time
from html.parser import HTMLParser
from pathlib import Path

from oddsfox.http import DOCUMENT_HOSTS, resolve_public, stream_get, validate_https_url

MAX_BYTES = 8 * 1024 * 1024
MAX_TEXT = 2 * 1024 * 1024
MEMORY_LIMIT = 1024**3
OFFICIAL_HOSTS = DOCUMENT_HOSTS


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
    host = validate_https_url(url, OFFICIAL_HOSTS)
    return resolve_public(host)


def retrieve(url: str) -> bytes:
    import httpx

    with httpx.Client(timeout=20, trust_env=False, follow_redirects=False) as client:
        return stream_get(
            client,
            url,
            allowed_hosts=OFFICIAL_HOSTS,
            follow_redirects=True,
            max_bytes=MAX_BYTES,
        )


def child_rss_bytes(pid: int) -> int:
    output = subprocess.check_output(["/bin/ps", "-o", "rss=", "-p", str(pid)], text=True)
    return int(output.strip().split()[0]) * 1024


def supervise_extractor(proc: subprocess.Popen, *, wall_seconds: int = 60) -> bytes:
    started = time.monotonic()
    while proc.poll() is None:
        if time.monotonic() - started > wall_seconds:
            proc.kill()
            raise ValueError("governing document could not be safely extracted")
        if sys.platform == "darwin":
            try:
                rss = child_rss_bytes(proc.pid)
            except subprocess.CalledProcessError, IndexError:
                rss = None
            if rss is not None and rss > MEMORY_LIMIT:
                proc.kill()
                raise ValueError("document extractor exceeded memory budget")
        time.sleep(0.2)
    stdout, _stderr = proc.communicate(timeout=5)
    if proc.returncode or len(stdout) > MAX_TEXT * 6:
        raise ValueError("governing document could not be safely extracted")
    return stdout


def extract_bounded(raw: bytes) -> str:
    # Parsing runs in a disposable process: malformed PDFs cannot monopolize the worker.
    with tempfile.TemporaryDirectory(prefix="oddsfox-document-") as directory:
        path = Path(directory) / "source"
        path.write_bytes(raw)
        proc = subprocess.Popen(
            [sys.executable, "-m", "oddsfox.documents", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = supervise_extractor(proc)
        return json.loads(stdout)


def capture_document(store, url: str) -> dict:
    """Failures are evidence too; never silently replace complete material with a guess."""
    import httpx

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
