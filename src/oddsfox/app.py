"""Loopback-only HTML and API, sharing the single writer with one worker."""

import hmac
import json
import secrets
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import Field

from oddsfox.catalog import event_detail, event_list, sync_status
from oddsfox.compiler import blank_ir, prepare_compile, run_compile_job
from oddsfox.ingest import fetch, import_capture
from oddsfox.ir import SemanticIR, StrictModel, Text, strict_json
from oddsfox.pipeline import Pipeline
from oddsfox.store import StaleInput, Store
from oddsfox.sync import SyncRunner


class ReviewRequest(StrictModel):
    reviewer: Text
    rationale: Text
    approve: bool
    governing_material_complete: bool = False


class RegistryRequest(StrictModel):
    canonical_id: Text
    observation: dict
    reviewer: Text
    rationale: Text
    aliases: list[dict] = Field(default_factory=list, max_length=50)


class ImportRequest(StrictModel):
    platform: str
    payload: str
    documents: list[dict] = Field(default_factory=list, max_length=30)


class CaptureRequest(StrictModel):
    platform: str
    native_ids: list[str] = Field(min_length=1, max_length=250)


class InterpretRequest(StrictModel):
    ir: dict
    derivations: dict = Field(default_factory=dict)


class PauseRequest(StrictModel):
    paused: bool


class SyncRequest(StrictModel):
    venues: list[str] = Field(
        default_factory=lambda: ["kalshi", "polymarket"],
        min_length=1,
        max_length=2,
    )


class ModelRequest(StrictModel):
    contract_version_id: Text
    model_name: Text
    max_tokens: int = Field(default=8192, ge=256, le=8192)
    constrained: bool = True


def create_app(
    store: Store,
    *,
    token: str | None = None,
    port: int = 8777,
    models: dict[str, Path] | None = None,
    auto_sync: bool = True,
) -> FastAPI:
    token = token or secrets.token_urlsafe(32)
    allowed_host = f"127.0.0.1:{port}"
    origin = f"http://{allowed_host}"
    pipeline = Pipeline(store)
    worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oddsfox-worker")
    models = models or {}
    runner = SyncRunner(store, models)

    @asynccontextmanager
    async def lifespan(app):
        for job in store._rows("SELECT * FROM jobs WHERE state='pending' ORDER BY updated"):
            if job["stage"] == "capture":
                body = CaptureRequest.model_validate(
                    {k: v for k, v in job["config"].items() if k != "requested_at"}
                )
                worker.submit(capture_job, job["id"], body)
            elif job["stage"] == "interpret":
                identity = job["config"].get("model", {}).get("identity")
                if identity in models:
                    worker.submit(run_compile_job, store, job["id"], models[identity])
        if auto_sync:
            runner.start()
        else:
            for _ in pipeline.comparison_contexts():
                pipeline.refresh_comparisons()
            runner.start(discovery=False)
        yield
        runner.close()
        worker.shutdown(wait=True, cancel_futures=False)

    app = FastAPI(
        title="OddsFox", version="0.1.0", lifespan=lifespan, docs_url=None, redoc_url=None
    )

    @app.middleware("http")
    async def guard(request: Request, call_next):
        if request.headers.get("host") != allowed_host:
            return JSONResponse({"detail": "untrusted host"}, status_code=400)
        if (
            request.headers.get("origin") not in {None, origin}
            or request.headers.get("sec-fetch-site") == "cross-site"
        ):
            return JSONResponse({"detail": "untrusted origin"}, status_code=403)
        if request.method not in {"GET", "HEAD", "OPTIONS"}:
            if not hmac.compare_digest(request.headers.get("x-oddsfox-token", ""), token):
                return JSONResponse({"detail": "session token required"}, status_code=403)
            body = bytearray()
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > 4 * 1024 * 1024:
                    return JSONResponse({"detail": "request exceeds 4 MiB"}, status_code=413)
            request._body = bytes(body)
            if body:
                try:
                    strict_json(request._body)
                except ValueError as exc:
                    return JSONResponse({"detail": str(exc)}, status_code=422)
        response = await call_next(request)
        response.headers.update(
            {
                "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
                "X-Content-Type-Options": "nosniff",
                "Referrer-Policy": "no-referrer",
                "Cache-Control": "no-store",
            }
        )
        return response

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse(
            {"detail": str(exc)}, status_code=409 if isinstance(exc, StaleInput) else 422
        )

    @app.get("/", response_class=HTMLResponse)
    def report():
        return (Path(__file__).parent / "static" / "index.html").read_text()

    @app.get("/research", response_class=HTMLResponse)
    def research():
        return (Path(__file__).parent / "static" / "research.html").read_text()

    @app.get("/assets/{name}")
    def asset(name: str):
        if name not in {"app.js", "events.js", "style.css"}:
            raise HTTPException(404)
        return Response(
            (Path(__file__).parent / "static" / name).read_bytes(),
            media_type="text/javascript" if name.endswith(".js") else "text/css",
        )

    @app.get("/api/events")
    def events(
        venue: str | None = None,
        category: str | None = None,
        qualification: str = "qualified",
        analysis: str | None = None,
        offset: int = 0,
        limit: int = 50,
    ):
        return event_list(
            store,
            venue=venue,
            category=category,
            qualification=qualification,
            analysis=analysis,
            offset=offset,
            limit=limit,
        )

    @app.get("/api/events/{identity}")
    def event(identity: str):
        return event_detail(store, identity)

    @app.get("/api/sync")
    def sync_settings():
        return sync_status(store) | {
            "models": sorted(models),
            "formal_verification": pipeline.cached_comparisons(),
        }

    @app.post("/api/sync", status_code=202)
    def sync_now(body: SyncRequest):
        from oddsfox.discovery import VENUES

        if any(v not in VENUES for v in body.venues):
            raise ValueError("unknown venue")
        jobs = [runner.request(v, manual=True) for v in dict.fromkeys(body.venues)]
        with store.transaction():
            for v in body.venues:
                store.db.execute("UPDATE sync_state SET due=0 WHERE venue=?", [v])
        return {"jobs": jobs, "note": "Queued; resume automatic sync if paused."}

    @app.post("/api/sync/pause")
    def sync_pause(body: PauseRequest):
        runner.pause(body.paused)
        return sync_status(store)

    @app.get("/api/comparisons")
    def comparisons(offset: int = 0, limit: int = 50):
        if offset < 0 or not 1 <= limit <= 100:
            raise ValueError("invalid comparison page")
        cache = pipeline.cached_comparisons()
        return cache | {
            "items": pipeline.cached_rows(offset, limit),
            "offset": offset,
            "limit": limit,
        }

    @app.get("/api/schema")
    def schema():
        return SemanticIR.model_json_schema()

    @app.get("/api/status")
    def status():
        return store.status()

    @app.get("/api/report")
    def report_data():
        with store.lock:
            data = pipeline.export()
            cache = pipeline.cached_comparisons()
            data["comparison_status"] = cache
            data["comparisons"] = pipeline.cached_rows()
            from oddsfox.reasoning import differences

            data["near_matches"] = differences(
                [SemanticIR.model_validate(r["data"]["ir"]) for r in data["interpretations"][:250]]
            )
            data["near_match_coverage"] = {
                "processed": min(250, len(data["interpretations"])),
                "total": len(data["interpretations"]),
                "complete": len(data["interpretations"]) <= 250,
            }
            data["status"] = store.status()
            data["models"] = sorted(models)
            return data

    @app.get("/api/export")
    def export(history: bool = False):
        return pipeline.export(history)

    @app.get("/api/assertions")
    def assertions(history: bool = False):
        return pipeline.export(history)["assertions"]

    @app.get("/api/records/{identity}")
    def record(identity: str):
        return store.get(identity)

    @app.get("/api/artifacts/{identity}")
    def artifact(identity: str):
        try:
            return Response(store.artifact(identity), media_type="text/plain; charset=utf-8")
        except FileNotFoundError as exc:
            raise HTTPException(404, "unknown artifact") from exc

    @app.get("/api/contracts/{identity}/draft")
    def draft(identity: str):
        if store.get(identity)["kind"] != "contract":
            raise ValueError("expected contract")
        return blank_ir(identity)

    @app.get("/api/interpretations/{identity}/ir")
    def canonical_ir(identity: str):
        record = store.get(identity)
        if record["kind"] != "interpretation":
            raise ValueError("expected interpretation")
        return Response(
            SemanticIR.model_validate(record["data"]["ir"]).canonical(),
            media_type="application/json",
        )

    @app.get("/api/interpretations/{identity}/evidence")
    def excerpts(identity: str):
        from oddsfox.ir import pointer_value

        record = store.get(identity)
        if record["kind"] != "interpretation":
            raise ValueError("expected interpretation")
        ir = record["data"]["ir"]
        texts = store.source_texts(ir["contract_version_id"])
        result = []
        for pointer, evidence in ir["field_evidence"].items():
            spans = evidence["source_spans"] or record["data"]["derivations"].get(
                evidence["derivation_ref"], {}
            ).get("source_spans", [])
            result.append(
                {
                    "field": pointer,
                    "value": pointer_value(ir, pointer),
                    "derivation": evidence["derivation_ref"],
                    "sources": [
                        span | {"text": texts[span["artifact_id"]][span["start"] : span["end"]]}
                        for span in spans
                    ],
                }
            )
        return result

    @app.post("/api/import")
    def import_(body: ImportRequest):
        return {"id": import_capture(store, body.platform, body.payload.encode(), body.documents)}

    def capture_job(identity, request):
        if store.claim_job(identity) is None:
            return
        try:
            results = fetch(store, request.platform, request.native_ids)
            if any(row["state"] == "failed" for row in results):
                raise ValueError("capture incomplete; see persisted refresh failures")
            with store.transaction():
                output = store.insert(
                    "capture_batch",
                    identity,
                    {"results": results},
                    [r["version_id"] for r in results],
                    "CAPTURED",
                )
                store.complete_job(identity, output)
        except Exception as exc:
            store.fail_job(identity, f"{type(exc).__name__}: {str(exc)[:1500]}")

    @app.post("/api/comparisons/refresh", status_code=202)
    def recompute():
        runner.launch_lane("proof_future", "proofs", runner.proofs, pipeline.refresh_comparisons)
        return {"state": "queued"}

    @app.post("/api/capture", status_code=202)
    def capture(body: CaptureRequest):
        from oddsfox.ingest import ENDPOINTS

        if body.platform not in ENDPOINTS:
            raise ValueError("unsupported venue")
        from oddsfox.store import now

        # An explicit refresh is a new retrieval; retries use the same persisted job.
        identity = store.enqueue("capture", [], body.model_dump() | {"requested_at": now()})
        worker.submit(capture_job, identity, body)
        return {"job_id": identity}

    @app.post("/api/interpret")
    def interpret(body: InterpretRequest):
        return {"id": pipeline.interpret(json.dumps(body.ir), derivations=body.derivations)}

    @app.post("/api/registry")
    def registry(body: RegistryRequest):
        return {
            "id": pipeline.register(
                body.canonical_id, body.observation, body.reviewer, body.rationale, body.aliases
            )
        }

    @app.post("/api/reviews/{identity}")
    def review(identity: str, body: ReviewRequest):
        return {"id": pipeline.review(identity, **body.model_dump())}

    @app.post("/api/publish")
    def publish():
        return {"assertions": pipeline.publish()}

    @app.post("/api/compile", status_code=202)
    def compile_(body: ModelRequest):
        if body.model_name not in models:
            raise ValueError("model is not configured by the local operator")
        store.require_current([body.contract_version_id])
        identity = prepare_compile(
            store,
            body.contract_version_id,
            models[body.model_name],
            constrained=body.constrained,
            max_tokens=body.max_tokens,
        )
        worker.submit(run_compile_job, store, identity, models[body.model_name])
        return {
            "state": "queued",
            "job_id": identity,
            "contract_version_id": body.contract_version_id,
        }

    @app.post("/api/jobs/{identity}/retry", status_code=202)
    def retry(identity: str):
        rows = store._rows("SELECT * FROM jobs WHERE id=?", [identity])
        if not rows or rows[0]["state"] != "pending":
            raise ValueError("job is not retryable; exhausted jobs retain failures")
        job = rows[0]
        if job["stage"] == "capture":
            body = CaptureRequest.model_validate(
                {k: v for k, v in job["config"].items() if k != "requested_at"}
            )
            worker.submit(capture_job, identity, body)
        else:
            raise ValueError(
                "retry inference using the identical configured model and compile action"
            )
        return {"job_id": identity}

    return app
