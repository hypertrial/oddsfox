"""One-time schema 4 removal of the retired Polymarket US integration."""

from oddsfox.ir import fingerprint

RETIRED = "polymarket_us"


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from strings(child)


def belongs(row):
    return (
        row.get("venue") == RETIRED
        or row.get("platform") == RETIRED
        or str(row.get("logical", "")).startswith(RETIRED + ":")
        or any(
            belongs(row.get(key, {}))
            for key in ("data", "config")
            if isinstance(row.get(key), dict)
        )
    )


def retired_chunks(store, nodes, jobs):
    """Opaque chunk cache keys must be reconstructed, including gaps-only outputs."""
    from oddsfox.explanations import chunks

    configs = {n["id"] for n in nodes if n["kind"] == "analysis_config"}
    configs.update(i for j in jobs if j["stage"] == "explain" for i in j["inputs"])
    removed, retained = set(), set()
    for node in nodes:
        if node["kind"] != "event_semantics":
            continue
        target = removed if belongs(node) else retained
        for spans in chunks(store, node["id"]):
            target.update(fingerprint({"spans": spans, "config": c}) for c in configs)
    return removed - retained


def purge_us(store):
    """Caller owns the transaction and has published a verified pre-purge backup."""
    before = store.referenced_artifacts()
    nodes = store._rows("SELECT * FROM nodes")
    jobs = store._rows("SELECT * FROM jobs")
    events = store._rows("SELECT * FROM events")
    dependencies = store._rows("SELECT * FROM dependencies")
    chunk_keys = retired_chunks(store, nodes, jobs)
    removed_nodes = {n["id"] for n in nodes if belongs(n)}
    removed_jobs = {j["id"] for j in jobs if belongs(j)}
    removed_events = {e["id"] for e in events if belongs(e)}
    removed_nodes.update(
        n["id"] for n in nodes if n["kind"] == "explanation_chunk" and n["logical"] in chunk_keys
    )
    removed_jobs.update(
        j["id"] for j in jobs if j["stage"] == "explain" and j["config"].get("chunk") in chunk_keys
    )
    # Follow references toward consumers, never into shared configuration/evidence parents.
    while True:
        previous = (len(removed_nodes), len(removed_jobs))
        identities = removed_nodes | removed_jobs | removed_events
        removed_nodes.update(d["child"] for d in dependencies if d["parent"] in removed_nodes)
        for node in nodes:
            if (
                identities.intersection(strings(node["data"]))
                or node["logical"] in identities
                or (
                    node["kind"] in {"discovery_page", "measurement"}
                    and node["logical"].rsplit(":", 1)[0] in removed_jobs
                )
            ):
                removed_nodes.add(node["id"])
        affected_matches = {
            n["logical"] for n in nodes if n["kind"] == "suggestions" and n["id"] in removed_nodes
        }
        removed_nodes.update(
            n["id"]
            for n in nodes
            if n["kind"] == "suggestions" and n["logical"] in affected_matches
        )
        removed_jobs.update(
            j["id"]
            for j in jobs
            if j["stage"] == "match" and affected_matches.intersection(j["inputs"])
        )
        for job in jobs:
            if identities.intersection(strings([job["inputs"], job["config"], job["output"]])):
                removed_jobs.add(job["id"])
        if previous == (len(removed_nodes), len(removed_jobs)):
            break
    identities = removed_nodes | removed_jobs | removed_events
    groups = {
        fingerprint(n["data"]["ir"]["observation"])
        for n in nodes
        if n["id"] in removed_nodes and n["kind"] == "interpretation"
    }
    signatures = {
        g["signature"]
        for g in store._rows("SELECT * FROM comparison_groups")
        if g["group_id"] in groups
    }
    for row in store._rows("SELECT * FROM comparison_rows"):
        if row["signature"] in signatures or identities.intersection(strings(row["data"])):
            store.db.execute(
                "DELETE FROM comparison_rows WHERE signature=? AND id=?",
                [row["signature"], row["id"]],
            )
    for group in groups:
        store.db.execute("DELETE FROM comparison_groups WHERE group_id=?", [group])
    # Work tables permit bound set deletion without constructing unbounded IN clauses.
    for name, values in (
        ("purged_nodes", removed_nodes),
        ("purged_jobs", removed_jobs),
        ("purged_events", removed_events),
    ):
        store.db.execute(f"CREATE TEMP TABLE {name} (id VARCHAR PRIMARY KEY)")
        if values:
            store.db.executemany(f"INSERT INTO {name} VALUES (?)", [(v,) for v in values])
    for table, condition in (
        (
            "dependencies",
            "child IN (SELECT id FROM purged_nodes) OR parent IN (SELECT id FROM purged_nodes)",
        ),
        ("snapshots", "version_id IN (SELECT id FROM purged_nodes) OR starts_with(logical, ? )"),
        (
            "semantic_heads",
            "version_id IN (SELECT id FROM purged_nodes) OR starts_with(logical, ? )",
        ),
        ("refreshes", "version_id IN (SELECT id FROM purged_nodes) OR starts_with(logical, ? )"),
        ("event_terms", "event_id IN (SELECT id FROM purged_events)"),
        ("events", "id IN (SELECT id FROM purged_events)"),
        ("sync_state", "venue=?"),
        ("attempts", "job_id IN (SELECT id FROM purged_jobs)"),
        ("jobs", "id IN (SELECT id FROM purged_jobs)"),
        ("nodes", "id IN (SELECT id FROM purged_nodes)"),
    ):
        params = [RETIRED if table == "sync_state" else RETIRED + ":"] if "?" in condition else []
        store.db.execute(f"DELETE FROM {table} WHERE {condition}", params)
    # A supported capture batch can depend on a removed result. Keep its venue's
    # scheduler operational if such a completed job was removed by the closure.
    store.db.execute(
        "UPDATE sync_state SET job_id=NULL,due=0 WHERE job_id IN (SELECT id FROM purged_jobs)"
    )
    after = store.referenced_artifacts()
    for artifact in before - after:
        store.db.execute(
            "INSERT INTO artifact_cleanup VALUES (?) ON CONFLICT DO NOTHING", [artifact]
        )
    return {
        "removed_nodes": len(removed_nodes),
        "removed_jobs": len(removed_jobs),
        "removed_events": len(removed_events),
        "removed_artifacts": len(before - after),
    }
