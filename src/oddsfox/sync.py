"""Durable, coalesced discovery and bounded independent local processing lanes."""

import json
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

from oddsfox.catalog import set_sync_state
from oddsfox.discovery import VENUES, native_id, pages, save_event
from oddsfox.store import now


class SyncRunner:
    def __init__(self, store, models=None, *, client_factory=None):
        self.store = store
        self.models = models or {}
        self.client_factory = client_factory or (
            lambda: httpx.Client(timeout=20, trust_env=False, follow_redirects=False)
        )
        self.stop_event = threading.Event()
        self.network = ThreadPoolExecutor(max_workers=3, thread_name_prefix="oddsfox-discovery")
        self.analysis = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oddsfox-explanations")
        self.proofs = ThreadPoolExecutor(max_workers=1, thread_name_prefix="oddsfox-proofs")
        self.futures = {}
        self.analysis_future = None
        self.proof_future = None
        self.thread = None
        self.engine = None
        self.discovery_enabled = True
        with store.transaction():
            for venue in VENUES:
                store.db.execute(
                    "INSERT INTO sync_state VALUES (?,NULL,0,false,?) ON CONFLICT DO NOTHING",
                    [venue, json.dumps({"state": "never", "pages": 0, "events": 0, "errors": []})],
                )

    def request(self, venue, *, manual=False):
        if venue not in VENUES:
            raise ValueError("unknown venue")
        with self.store.lock:
            state = self.store._rows("SELECT * FROM sync_state WHERE venue=?", [venue])[0]
            if state["job_id"]:
                jobs = self.store._rows("SELECT state FROM jobs WHERE id=?", [state["job_id"]])
                if jobs and jobs[0]["state"] in {"pending", "running"}:
                    if manual:
                        set_sync_state(self.store, venue, manual=True)
                    return state["job_id"]
            identity = self.store.enqueue(
                "discover", [], {"venue": venue, "generation": str(uuid.uuid4())}
            )
            with self.store.transaction():
                self.store.db.execute(
                    "UPDATE sync_state SET job_id=?,data=? WHERE venue=?",
                    [
                        identity,
                        json.dumps(
                            {
                                "state": "pending",
                                "manual": manual,
                                "pages": 0,
                                "events": 0,
                                "errors": [],
                                "error_count": 0,
                                "last_success": state["data"].get("last_success"),
                            }
                        ),
                        venue,
                    ],
                )
            return identity

    def pause(self, paused):
        with self.store.transaction():
            self.store.db.execute("UPDATE sync_state SET paused=?", [paused])

    def run_venue(self, venue, identity):
        job = self.store.claim_job(identity)
        if job is None:
            return
        state = self.store._rows("SELECT data FROM sync_state WHERE venue=?", [venue])[0]["data"]
        set_sync_state(self.store, venue, state="running", started_at=now())
        cache = {}
        try:
            with self.client_factory() as client:
                for events, raw, url, checkpoint in pages(client, venue, state.get("checkpoint")):
                    artifact = self.store.put_artifact(raw)
                    with self.store.transaction():
                        self.store.insert(
                            "discovery_page",
                            identity + ":" + str(state["pages"]),
                            {"raw_artifact": artifact, "source_url": url, "checkpoint": checkpoint},
                            [],
                            "CAPTURED",
                            make_current=False,
                        )
                    for event in events:
                        if (
                            self.stop_event.is_set()
                            or self.store._rows(
                                "SELECT paused FROM sync_state WHERE venue=?", [venue]
                            )[0]["paused"]
                        ):
                            with self.store.transaction():
                                self.store.db.execute(
                                    "UPDATE jobs SET state='pending',attempts=attempts-1 WHERE id=?",
                                    [identity],
                                )
                            set_sync_state(
                                self.store,
                                venue,
                                state="paused" if not self.stop_event.is_set() else "interrupted",
                            )
                            return
                        key = f"{venue}:{native_id(event, venue)}"
                        if self.store._rows(
                            "SELECT id FROM events WHERE id=? AND seen_run=?", [key, identity]
                        ):
                            continue
                        try:
                            save_event(
                                self.store, client, venue, event, identity, artifact, url, cache
                            )
                            state["events"] += 1
                        except (ValueError, OSError, httpx.HTTPError) as exc:
                            state["error_count"] = state.get("error_count", 0) + 1
                            state["errors"] = (state["errors"] + [f"{key}: {str(exc)[:250]}"])[-20:]
                    state["pages"] += 1
                    state["checkpoint"] = checkpoint
                    set_sync_state(self.store, venue, **(state | {"state": "running"}))
            with self.store.transaction():
                if not state.get("error_count"):
                    self.store.db.execute(
                        "UPDATE events SET active=false WHERE venue=? AND seen_run<>?",
                        [venue, identity],
                    )
                output = self.store.insert(
                    "discovery_run",
                    identity,
                    state,
                    [],
                    "PARTIAL" if state.get("error_count") else "COMPLETE",
                    make_current=False,
                )
                self.store.complete_job(identity, output)
                self.store.db.execute(
                    "UPDATE sync_state SET due=? WHERE venue=?", [time.time() + 900, venue]
                )
            set_sync_state(
                self.store,
                venue,
                state="partial" if state.get("error_count") else "complete",
                completed_at=now(),
                manual=False,
                last_success=state.get("last_success") if state.get("error_count") else now(),
            )
        except Exception as exc:
            self.store.fail_job(identity, str(exc))
            set_sync_state(self.store, venue, state="failed", diagnostic=str(exc)[:1000])
            with self.store.transaction():
                self.store.db.execute(
                    "UPDATE sync_state SET due=? WHERE venue=?", [time.time() + 60, venue]
                )

    def tick(self):
        for state in self.store._rows("SELECT * FROM sync_state"):
            if not self.discovery_enabled and not state["data"].get("manual"):
                continue
            venue = state["venue"]
            future = self.futures.get(venue)
            if (
                state["paused"]
                or (future is not None and not future.done())
                or state["due"] > time.time()
            ):
                continue
            job = self.request(venue)
            self.futures[venue] = self.network.submit(self.run_venue, venue, job)
        if self.models:
            self.launch_lane("analysis_future", "analysis", self.analysis, self.analyze_next)
        from oddsfox.pipeline import Pipeline

        self.launch_lane(
            "proof_future", "proofs", self.proofs, Pipeline(self.store).refresh_comparisons
        )

    def launch_lane(self, attribute, lane, executor, work):
        with self.store.lock:
            future = getattr(self, attribute)
            if future is not None:
                if not future.done():
                    return
                try:
                    future.result()
                    state, diagnostic, retry = "idle", "", 0
                except Exception as exc:
                    state, diagnostic, retry = "failed", str(exc)[:1000], time.time() + 60
                with self.store.transaction():
                    self.store.db.execute(
                        "INSERT INTO lane_state VALUES (?,?,?,?) ON CONFLICT(lane) DO UPDATE SET state=excluded.state,diagnostic=excluded.diagnostic,retry_at=excluded.retry_at",
                        [lane, state, diagnostic, retry],
                    )
                setattr(self, attribute, None)
            rows = self.store._rows("SELECT * FROM lane_state WHERE lane=?", [lane])
            if rows and rows[0]["retry_at"] > time.time():
                return
            with self.store.transaction():
                self.store.db.execute(
                    "INSERT INTO lane_state VALUES (?,'running','',0) ON CONFLICT(lane) DO UPDATE SET state='running'",
                    [lane],
                )
            setattr(self, attribute, executor.submit(work))

    def analyze_next(self):
        if not self.models:
            return
        from oddsfox.explanations import AnalysisEngine

        if self.engine is None:
            self.engine = AnalysisEngine(self.store, next(iter(self.models.values())))
        self.engine.step()

    def start(self, *, discovery=True):
        self.discovery_enabled = discovery
        if self.thread is not None:
            return
        # A restart refreshes immediately, preserving a resumable interrupted run.
        with self.store.transaction():
            self.store.db.execute("UPDATE sync_state SET due=0")

        def loop():
            while not self.stop_event.is_set():
                try:
                    self.tick()
                except Exception as exc:
                    # Keep the scheduler alive, and expose orchestration failures.
                    for venue in VENUES:
                        set_sync_state(self.store, venue, scheduler_error=str(exc)[:500])
                self.stop_event.wait(2)

        self.thread = threading.Thread(target=loop, daemon=True, name="oddsfox-scheduler")
        self.thread.start()

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join()
        self.network.shutdown(wait=True, cancel_futures=True)
        self.analysis.shutdown(wait=True, cancel_futures=True)
        self.proofs.shutdown(wait=True, cancel_futures=True)
