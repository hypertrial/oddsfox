"""One coordinator owns DuckDB and every current-version transition."""

import builtins
import fcntl
import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import duckdb

from oddsfox.ir import fingerprint

EVENT_COLUMNS = """id VARCHAR PRIMARY KEY, venue VARCHAR NOT NULL, native_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL, category VARCHAR NOT NULL, volume VARCHAR,
    qualification VARCHAR NOT NULL, active BOOLEAN NOT NULL, seen_run VARCHAR NOT NULL,
    semantic_id VARCHAR, data JSON NOT NULL, updated VARCHAR NOT NULL"""


def now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class StaleInput(ValueError):
    pass


def volume_order_key(amount: str | None) -> str | None:
    """Internal exact numeric ordering for nonnegative event totals below 1e26."""
    if amount is None:
        return None
    try:
        number = Decimal(amount)
    except InvalidOperation as exc:
        raise ValueError("invalid event volume") from exc
    if not number.is_finite() or number < 0 or number >= Decimal("1e26"):
        raise ValueError("invalid event volume")
    # Fixed-point formatting is exact, independent of the active decimal context.
    whole, _, fraction = format(number if number else Decimal(0), "f").partition(".")
    return whole.zfill(26) + "." + fraction.rstrip("0")


def governing(data: dict) -> str:
    """Semantic identity excludes quote/volume/retrieval and trading-state changes."""
    metadata = data["metadata"]
    return fingerprint(
        {
            "platform": data["platform"],
            "native_id": data["native_id"],
            "texts": data["text_artifacts"],
            "references": data.get("references", []),
            "metadata": {
                k: v
                for k, v in metadata.items()
                if v != {}
                and k
                not in {
                    "lifecycle",
                    "source_effective_time",
                    "volume",
                    "volume_basis",
                    "retrieved_at",
                    "event_page_artifact",
                }
            },
        }
    )


class Store:
    def __init__(self, directory: str | Path, *, _maintenance=False):
        self.directory = Path(directory).resolve()
        exists = (self.directory / "oddsfox.duckdb").is_file()
        if _maintenance and not exists:
            raise ValueError("dataset does not exist")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.artifacts = self.directory / "artifacts"
        self.artifacts.mkdir(exist_ok=True, mode=0o700)
        self.lock = threading.RLock()
        self._process_lock = (self.directory / "writer.lock").open("a+")
        try:
            fcntl.flock(self._process_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._process_lock.close()
            raise RuntimeError("OddsFox is already running for this dataset; use its API") from exc
        try:
            self.db = duckdb.connect(str(self.directory / "oddsfox.duckdb"))
            if exists:
                try:
                    versions = self.db.execute("SELECT version FROM metadata").fetchall()
                except duckdb.Error as exc:
                    raise ValueError("dataset has no readable schema version") from exc
                if versions not in ([(1,)], [(2,)], [(3,)], [(4,)]):
                    raise ValueError("unsupported database version; preserve dataset")
                self.version = versions[0][0]
                if _maintenance:
                    return
                if self.version != 4:
                    raise ValueError(
                        "dataset migration required: stop the app, then run oddsfox --data "
                        f"'{self.directory}' migrate --backup <new-backup-directory>"
                    )
            else:
                self.version = 4
                with self.transaction():
                    self.db.execute("CREATE TABLE metadata (version INTEGER PRIMARY KEY)")
                    self.db.execute("INSERT INTO metadata VALUES (4)")
                    self._initialize_schema(4)
            self.cleanup_artifacts()
            with self.transaction():
                self.db.execute(
                    "INSERT INTO attempts SELECT id,attempts,'interrupted','process stopped before completion',NULL,? FROM jobs WHERE state='running'",
                    [now()],
                )
                self.db.execute(
                    "UPDATE jobs SET state=CASE WHEN attempts>=3 THEN 'failed' ELSE 'pending' END, diagnostic='interrupted; recovered on restart' WHERE state='running'"
                )
        except BaseException:
            self.close()
            raise

    def _initialize_schema(self, version):
        self.db.execute("""CREATE TABLE IF NOT EXISTS nodes (
            id VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL, logical VARCHAR NOT NULL,
            data JSON NOT NULL, current BOOLEAN NOT NULL, status VARCHAR NOT NULL,
            created VARCHAR NOT NULL, reason VARCHAR NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS dependencies (
            child VARCHAR NOT NULL, parent VARCHAR NOT NULL, PRIMARY KEY(child,parent))""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS refreshes (
            logical VARCHAR NOT NULL, version_id VARCHAR, retrieved VARCHAR NOT NULL,
            success BOOLEAN NOT NULL, diagnostic VARCHAR NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id VARCHAR PRIMARY KEY, stage VARCHAR NOT NULL, inputs JSON NOT NULL,
            config JSON NOT NULL, state VARCHAR NOT NULL, attempts INTEGER NOT NULL,
            output VARCHAR, diagnostic VARCHAR NOT NULL, updated VARCHAR NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS attempts (
            job_id VARCHAR NOT NULL, number INTEGER NOT NULL, state VARCHAR NOT NULL,
            diagnostic VARCHAR NOT NULL, artifact VARCHAR, created VARCHAR NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS snapshots (
            logical VARCHAR NOT NULL, version_id VARCHAR NOT NULL,
            artifact VARCHAR NOT NULL, data JSON NOT NULL, retrieved VARCHAR NOT NULL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS semantic_heads (
            logical VARCHAR PRIMARY KEY, digest VARCHAR NOT NULL, version_id VARCHAR NOT NULL)""")
        self.db.execute(f"CREATE TABLE IF NOT EXISTS events ({EVENT_COLUMNS})")
        self.db.execute("""CREATE TABLE IF NOT EXISTS event_terms (
            term VARCHAR NOT NULL, event_id VARCHAR NOT NULL, PRIMARY KEY(term,event_id))""")
        self.db.execute("CREATE INDEX IF NOT EXISTS event_term_lookup ON event_terms(term)")
        self.db.execute("""CREATE TABLE IF NOT EXISTS sync_state (
            venue VARCHAR PRIMARY KEY, job_id VARCHAR, due DOUBLE NOT NULL,
            paused BOOLEAN NOT NULL, data JSON NOT NULL)""")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS comparison_rows (signature VARCHAR, id VARCHAR, data JSON, PRIMARY KEY(signature,id))"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS comparison_groups (group_id VARCHAR PRIMARY KEY, signature VARCHAR, state VARCHAR, processed INTEGER, pair_cursor INTEGER, updated DOUBLE)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS lane_state (lane VARCHAR PRIMARY KEY, state VARCHAR, diagnostic VARCHAR, retry_at DOUBLE)"
        )
        for row in self.list("contract"):
            self.db.execute(
                "INSERT INTO semantic_heads VALUES (?,?,?) ON CONFLICT DO NOTHING",
                [row["logical"], governing(row["data"]), row["id"]],
            )
        if version < 3:
            # DuckDB cannot alter an indexed type after DROP INDEX in one
            # transaction. Replace the table atomically, retaining its keys.
            self.db.execute(f"CREATE TABLE events_v3 ({EVENT_COLUMNS})")
            self.db.execute("INSERT INTO events_v3 SELECT * FROM events")
            for row in self._rows("SELECT id,data FROM events"):
                self.db.execute(
                    "UPDATE events_v3 SET volume=? WHERE id=?",
                    [volume_order_key(row["data"]["volume"]["amount"]), row["id"]],
                )
            self.db.execute("DROP TABLE events")
            self.db.execute("ALTER TABLE events_v3 RENAME TO events")
        self.db.execute("CREATE INDEX IF NOT EXISTS event_catalog_order ON events(volume)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS artifact_cleanup (artifact VARCHAR PRIMARY KEY)"
        )

    def close(self):
        with self.lock:
            if hasattr(self, "db"):
                self.db.close()
            fcntl.flock(self._process_lock, fcntl.LOCK_UN)
            self._process_lock.close()

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute("BEGIN TRANSACTION")
            try:
                yield
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def put_artifact(self, content: bytes) -> str:
        digest = hashlib.sha256(content).hexdigest()
        target = self.artifacts / digest
        if target.exists():
            if target.read_bytes() != content:
                raise ValueError("immutable artifact integrity failure")
            return digest
        descriptor, staging = tempfile.mkstemp(prefix=".stage-", dir=self.artifacts)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(staging, target)
            directory_fd = os.open(self.artifacts, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(staging):
                os.unlink(staging)
        return digest

    def artifact(self, artifact_id: str) -> bytes:
        if not re.fullmatch(r"[a-f0-9]{64}", artifact_id):
            raise ValueError("invalid artifact ID")
        raw = (self.artifacts / artifact_id).read_bytes()
        if hashlib.sha256(raw).hexdigest() != artifact_id:
            raise ValueError("artifact digest mismatch")
        return raw

    def _rows(self, sql: str, args=()) -> builtins.list[dict]:
        with self.lock:
            cursor = self.db.execute(sql, args)
            names = [x[0] for x in cursor.description]
            rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
            for row in rows:
                for key in ("data", "inputs", "config"):
                    if key in row and isinstance(row[key], str):
                        row[key] = json.loads(row[key])
            return rows

    def get(self, identity: str) -> dict:
        result = self._rows("SELECT * FROM nodes WHERE id=?", [identity])
        if not result:
            raise ValueError("unknown record")
        return result[0]

    def list(self, kind: str, history: bool = False) -> builtins.list[dict]:
        return self._rows(
            "SELECT * FROM nodes WHERE kind=? AND (current OR ?) ORDER BY created,id",
            [kind, history],
        )

    def current(self, kind: str, logical: str) -> dict | None:
        rows = self._rows(
            "SELECT * FROM nodes WHERE kind=? AND logical=? AND current", [kind, logical]
        )
        if len(rows) > 1:
            raise RuntimeError("multiple current versions violate dataset integrity")
        return rows[0] if rows else None

    def require_current(self, identities: builtins.list[str]):
        for identity in identities:
            if not self.get(identity)["current"]:
                raise StaleInput(f"dependency {identity} is no longer current")

    def invalidate(self, identity: str, reason: str):
        """Called inside a write transaction before replacing a current pointer."""
        self.db.execute(
            """WITH RECURSIVE affected(id) AS (
            SELECT ? UNION SELECT d.child FROM dependencies d JOIN affected a ON d.parent=a.id
            ) UPDATE nodes SET current=false, status='STALE', reason=? WHERE id IN (SELECT id FROM affected)""",
            [identity, reason],
        )

    def insert(
        self,
        kind: str,
        logical: str,
        data: dict,
        dependencies: builtins.list[str],
        status: str = "PROVISIONAL",
        make_current: bool = True,
    ) -> str:
        """Only called inside transaction; idempotent immutable payloads, mutable lifecycle."""
        identity = fingerprint(
            {
                "kind": kind,
                "logical": logical,
                "data": data,
                "dependencies": sorted(set(dependencies)),
            }
        )
        existing = self._rows("SELECT id,current FROM nodes WHERE id=?", [identity])
        if make_current:
            self.require_current(dependencies)
        if existing:
            # Obsolete jobs cannot resurrect a previously invalidated output.
            if make_current and not existing[0]["current"]:
                raise StaleInput(
                    "historical output cannot be restored; create a new reviewed revision"
                )
            return identity
        if make_current:
            old = self.current(kind, logical)
            if old:
                self.invalidate(old["id"], f"{kind} revision superseded")
        self.db.execute(
            "INSERT INTO nodes VALUES (?,?,?,?,?,?,?,?)",
            [
                identity,
                kind,
                logical,
                json.dumps(data, ensure_ascii=False),
                make_current,
                status,
                now(),
                "",
            ],
        )
        for parent in sorted(set(dependencies)):
            self.get(parent)
            self.db.execute("INSERT INTO dependencies VALUES (?,?)", [identity, parent])
        return identity

    def capture(
        self,
        platform: str,
        native_id: str,
        payload: bytes,
        texts: dict[str, str],
        metadata: dict,
        references: builtins.list[dict] | None = None,
    ) -> str:
        raw_id = self.put_artifact(payload)
        text_artifacts = {
            name: self.put_artifact(text.encode("utf-8")) for name, text in texts.items() if text
        }
        references = references or []
        data = {
            "platform": platform,
            "native_id": native_id,
            "payload_artifact": raw_id,
            "text_artifacts": text_artifacts,
            "metadata": metadata,
            "references": references,
        }
        logical = f"{platform}:{native_id}"
        with self.transaction():
            current = self.current("contract", logical)
            if (
                current
                and "event_rules" in current["data"]["text_artifacts"]
                and not texts.get("event_rules")
            ):
                raise ValueError(
                    "Known parent governing material cannot be omitted; refresh through event discovery"
                )
            digest = governing(data)
            heads = self._rows("SELECT digest FROM semantic_heads WHERE logical=?", [logical])
            if current and heads and heads[0]["digest"] == digest:
                identity = current["id"]
            else:
                # Capture sequence preserves a source reverting to a historical payload.
                data["capture_revision"] = (
                    len(
                        self._rows(
                            "SELECT id FROM nodes WHERE kind='contract' AND logical=?", [logical]
                        )
                    )
                    + 1
                )
                if current and {
                    k: v for k, v in current["data"].items() if k != "capture_revision"
                } == {k: v for k, v in data.items() if k != "capture_revision"}:
                    identity = current["id"]
                else:
                    identity = self.insert("contract", logical, data, [], "CAPTURED")
            self.db.execute(
                "INSERT INTO semantic_heads VALUES (?,?,?) ON CONFLICT(logical) DO UPDATE SET digest=excluded.digest,version_id=excluded.version_id",
                [logical, digest, identity],
            )
            self.db.execute(
                "INSERT INTO snapshots VALUES (?,?,?,?,?)",
                [
                    logical,
                    identity,
                    raw_id,
                    json.dumps({"metadata": metadata, "references": references}),
                    now(),
                ],
            )
            self.db.execute(
                "INSERT INTO refreshes VALUES (?,?,?,?,?)", [logical, identity, now(), True, ""]
            )
        return identity

    def refresh_failure(self, platform: str, native_id: str, diagnostic: str):
        with self.transaction():
            logical = f"{platform}:{native_id}"
            old = self.current("contract", logical)
            self.db.execute(
                "INSERT INTO refreshes VALUES (?,?,?,?,?)",
                [logical, old["id"] if old else None, now(), False, diagnostic[:2000]],
            )

    def source_texts(self, contract_id: str) -> dict[str, str]:
        data = self.get(contract_id)["data"]
        return {
            identity: self.artifact(identity).decode("utf-8")
            for identity in data["text_artifacts"].values()
        }

    def enqueue(self, stage: str, inputs: builtins.list[str], config: dict) -> str:
        identity = fingerprint({"stage": stage, "inputs": sorted(inputs), "config": config})
        with self.transaction():
            self.require_current(inputs)
            self.db.execute(
                "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [
                    identity,
                    stage,
                    json.dumps(inputs),
                    json.dumps(config),
                    "pending",
                    0,
                    None,
                    "",
                    now(),
                ],
            )
        return identity

    def claim_job(self, identity: str) -> dict | None:
        with self.transaction():
            rows = self._rows("SELECT * FROM jobs WHERE id=?", [identity])
            if not rows:
                raise ValueError("unknown job")
            job = rows[0]
            if job["state"] != "pending" or job["attempts"] >= 3:
                return None
            self.db.execute(
                "UPDATE jobs SET state='running', attempts=attempts+1, updated=? WHERE id=?",
                [now(), identity],
            )
            return job

    def fail_job(self, identity: str, diagnostic: str, artifact: str | None = None):
        with self.transaction():
            job = self._rows("SELECT * FROM jobs WHERE id=?", [identity])[0]
            self.db.execute(
                "INSERT INTO attempts VALUES (?,?,?,?,?,?)",
                [identity, job["attempts"], "failed", diagnostic[:2000], artifact, now()],
            )
            state = "failed" if job["attempts"] >= 3 else "pending"
            self.db.execute(
                "UPDATE jobs SET state=?,diagnostic=?,updated=? WHERE id=?",
                [state, diagnostic[:2000], now(), identity],
            )

    def complete_job(self, identity: str, output: str, artifact: str | None = None):
        # Stage output and completion share the caller's transaction.
        job = self._rows("SELECT * FROM jobs WHERE id=?", [identity])[0]
        if job["state"] != "running":
            raise ValueError("job is not running")
        self.require_current(job["inputs"])
        self.db.execute(
            "UPDATE jobs SET state='done',output=?,diagnostic='',updated=? WHERE id=?",
            [output, now(), identity],
        )
        self.db.execute(
            "INSERT INTO attempts VALUES (?,?,?,?,?,?)",
            [identity, job["attempts"], "done", "", artifact, now()],
        )

    def status(self) -> dict[str, Any]:
        with self.lock:
            return {
                "jobs": self._rows("SELECT * FROM jobs ORDER BY updated DESC"),
                "attempts": self._rows("SELECT * FROM attempts ORDER BY created DESC"),
                "measurements": self.list("measurement", True),
                "quarantined": self.list("quarantine", True),
                "refreshes": self._rows("SELECT * FROM refreshes ORDER BY retrieved DESC"),
                "states": self._rows(
                    "SELECT kind,status,count(*) AS count FROM nodes GROUP BY kind,status"
                ),
                "pending_review": len(
                    [x for x in self.list("interpretation") if not self.current("review", x["id"])]
                ),
                "limits": {"contracts_per_batch": 250, "attempts_per_job": 3},
            }

    def export_parquet(self, destination: Path, metadata: dict[str, Any]):
        if destination.exists():
            raise ValueError("export destination already exists")
        with self.lock:
            # DuckDB's path parameter is bound, never interpolated as SQL.
            self.db.execute(
                "COPY (SELECT * FROM nodes WHERE kind='assertion' AND current AND status='ACCEPTED') TO $destination (FORMAT PARQUET, KV_METADATA {oddsfox: $metadata})",
                {"destination": str(destination), "metadata": json.dumps(metadata)},
            )

    def backup(self, destination: Path):
        """Coordinator pauses writes while copying a checkpointed logical dataset."""
        with self.lock:
            if destination.exists() or destination.resolve().is_relative_to(self.directory):
                raise ValueError("backup needs a new directory outside the live dataset")
            self.verify_integrity()
            self.db.execute("CHECKPOINT")
            destination.parent.mkdir(parents=True, exist_ok=True)
            staging = Path(tempfile.mkdtemp(prefix=".oddsfox-backup-", dir=destination.parent))
            try:
                shutil.copy2(self.directory / "oddsfox.duckdb", staging / "oddsfox.duckdb")
                shutil.copytree(self.artifacts, staging / "artifacts")
                copied = Store(staging, _maintenance=True)
                try:
                    copied.verify_integrity()
                    for file in copied.artifacts.iterdir():
                        if not file.name.startswith(".stage-"):
                            copied.artifact(file.name)
                finally:
                    copied.close()
                staging.rename(destination)
            finally:
                if staging.exists():
                    shutil.rmtree(staging)

    @classmethod
    def restore(cls, source: Path, destination: Path):
        """Verify a private copy without upgrading it or opening the backup for writing."""
        source, destination = source.resolve(), destination.resolve()
        if destination.exists() or destination.is_relative_to(source):
            raise ValueError("restore needs a new directory outside the backup")
        if not (source / "oddsfox.duckdb").is_file() or not (source / "artifacts").is_dir():
            raise ValueError("backup dataset does not exist or is incomplete")
        with ExitStack() as locks:
            if (source / "writer.lock").exists():
                lock = locks.enter_context((source / "writer.lock").open("rb"))
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise RuntimeError("stop the source application before restoring") from exc
            else:
                # Legacy/manual copies may omit the process lock. DuckDB's read-only
                # connection prevents a concurrent writer without creating that file.
                try:
                    connection = duckdb.connect(str(source / "oddsfox.duckdb"), read_only=True)
                except duckdb.Error as exc:
                    raise ValueError("backup must be stopped and readable before restore") from exc
                locks.callback(connection.close)
            destination.parent.mkdir(parents=True, exist_ok=True)
            staging = Path(tempfile.mkdtemp(prefix=".oddsfox-restore-", dir=destination.parent))
            try:
                for name in ("oddsfox.duckdb", "oddsfox.duckdb.wal"):
                    if (source / name).exists():
                        shutil.copy2(source / name, staging / name)
                shutil.copytree(source / "artifacts", staging / "artifacts")
                copied = cls(staging, _maintenance=True)
                try:
                    copied.verify_integrity()
                    for file in copied.artifacts.iterdir():
                        if not file.name.startswith(".stage-"):
                            copied.artifact(file.name)
                finally:
                    copied.close()
                if destination.exists():
                    raise ValueError("restore destination already exists")
                staging.rename(destination)
                return copied.version
            finally:
                if staging.exists():
                    shutil.rmtree(staging)

    def referenced_artifacts(self):
        """All persisted references, including historical jobs and comparison caches."""
        with self.lock:
            required = set()

            def visit(value, key=""):
                if isinstance(value, dict):
                    for child_key, child in value.items():
                        if child_key in {"text_artifacts", "proof_artifacts"}:
                            required.update(child.values())
                        else:
                            visit(child, child_key)
                elif isinstance(value, list):
                    for child in value:
                        visit(child, key)
                elif isinstance(value, str) and (key.endswith("artifact") or key == "artifact_id"):
                    required.add(value)

            tables = self.tables()
            for table in ("nodes", "events", "snapshots", "jobs", "attempts", "comparison_rows"):
                if table in tables:
                    for row in self._rows(f"SELECT * FROM {table}"):
                        visit(row)
            return required

    def tables(self):
        return {row[0] for row in self.db.execute("SHOW TABLES").fetchall()}

    def verify_integrity(self):
        """Check referenced evidence, not just the files that happen to exist."""
        with self.lock:
            for identity in self.referenced_artifacts():
                try:
                    self.artifact(identity)
                except FileNotFoundError as exc:
                    raise ValueError(f"dataset is missing referenced artifact {identity}") from exc
            missing_row = self.db.execute(
                "SELECT count(*) FROM dependencies d LEFT JOIN nodes p ON p.id=d.parent LEFT JOIN nodes c ON c.id=d.child WHERE p.id IS NULL OR c.id IS NULL"
            ).fetchone()
            assert missing_row is not None
            if missing_row[0]:
                raise ValueError("dataset has dangling dependency references")
            for table, query in (
                (
                    "semantic_heads",
                    "SELECT count(*) FROM semantic_heads h LEFT JOIN nodes n ON h.version_id=n.id WHERE n.id IS NULL",
                ),
                (
                    "events",
                    "SELECT count(*) FROM events e LEFT JOIN nodes n ON e.semantic_id=n.id WHERE e.semantic_id IS NOT NULL AND n.id IS NULL",
                ),
                (
                    "snapshots",
                    "SELECT count(*) FROM snapshots s LEFT JOIN nodes n ON s.version_id=n.id WHERE n.id IS NULL",
                ),
            ):
                if table not in self.tables():
                    continue
                row = self.db.execute(query).fetchone()
                if row and row[0]:
                    raise ValueError("dataset has dangling discovery references")

    def cleanup_artifacts(self):
        """Resume committed deletion work before any operational jobs start."""
        pending = self._rows("SELECT artifact FROM artifact_cleanup")
        if not pending:
            return
        referenced = self.referenced_artifacts()
        for row in pending:
            identity = row["artifact"]
            if not re.fullmatch(r"[a-f0-9]{64}", identity):
                raise ValueError("invalid cleanup artifact ID")
            if identity not in referenced:
                (self.artifacts / identity).unlink(missing_ok=True)
            with self.transaction():
                self.db.execute("DELETE FROM artifact_cleanup WHERE artifact=?", [identity])

    @classmethod
    def migrate(cls, directory: Path, backup: Path):
        from oddsfox.migration import purge_us

        store = cls(directory, _maintenance=True)
        try:
            if store.version == 4:
                store.cleanup_artifacts()
                return {"version": 4, "migration_required": False}
            store.backup(backup)
            with store.transaction():
                store._initialize_schema(store.version)
                result = purge_us(store)
                store.verify_integrity()
                store.db.execute("UPDATE metadata SET version=4")
            store.cleanup_artifacts()
            return {"version": 4, "backup": str(backup.resolve()), **result}
        finally:
            store.close()
