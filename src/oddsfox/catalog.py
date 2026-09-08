"""Paginated catalog reads never invoke a model or solver."""

import json

from oddsfox.discovery import SCHEMA, VENUES, terms


def event_list(
    store,
    *,
    venue=None,
    category=None,
    qualification="qualified",
    analysis=None,
    offset=0,
    limit=50,
):
    if not 1 <= limit <= 100 or offset < 0:
        raise ValueError("limit must be 1..100 and offset nonnegative")
    if venue is not None and venue not in VENUES:
        raise ValueError("unknown venue")
    if qualification not in {"qualified", "unknown", "below_threshold", "all"}:
        raise ValueError("unknown qualification")
    if analysis not in {None, "ready", "pending"}:
        raise ValueError("analysis must be ready or pending")
    where, args = ["e.active"], []
    for field, value in [("venue", venue), ("category", category)]:
        if value is not None:
            where.append("e." + field + "=?")
            args.append(value)
    if qualification != "all":
        where.append("e.qualification=?")
        args.append(qualification)
    if analysis:
        where.append("x.id IS " + ("NOT NULL" if analysis == "ready" else "NULL"))
    source = (
        "FROM events e LEFT JOIN nodes x ON x.kind='explanation' AND x.logical=e.semantic_id AND x.current WHERE "
        + " AND ".join(where)
    )
    with store.lock:
        count = store._rows("SELECT count(*) AS total " + source, args)[0]["total"]
        rows = store._rows(
            "SELECT e.*,x.id AS explanation_id "
            + source
            + " ORDER BY e.volume DESC NULLS LAST,e.id LIMIT ? OFFSET ?",
            args + [limit, offset],
        )
    for row in rows:
        row["volume"] = row["data"]["volume"]["amount"]
        row["analysis_status"] = (
            "ready" if row["explanation_id"] else "pending" if row["semantic_id"] else "not_queued"
        )
    return {
        "schema_version": SCHEMA,
        "items": rows,
        "total": count,
        "offset": offset,
        "limit": limit,
        "next_offset": offset + len(rows) if offset + len(rows) < count else None,
    }


def event_detail(store, identity):
    with store.lock:
        rows = store._rows("SELECT * FROM events WHERE id=?", [identity])
        if not rows:
            raise ValueError("unknown event")
        row = rows[0]
        row["volume"] = row["data"]["volume"]["amount"]
        semantic = row["semantic_id"]
        row["explanation"] = store.current("explanation", semantic) if semantic else None
        row["suggestions"] = store.current("suggestions", semantic) if semantic else None
        row["semantic"] = store.get(semantic) if semantic else None
        contracts = row["data"]["contracts"]
        row["contracts"] = [store.get(c) for c in contracts]
        row["assertions"] = [
            a
            for a in store.list("assertion")
            if a["status"] == "ACCEPTED"
            and (a["data"]["a"] in contracts or a["data"]["b"] in contracts)
        ]
        return row


def candidates_for(store, event_id, extra_terms=()):
    """Indexed lexical/entity/date overlap; candidate coverage is explicitly non-exhaustive."""
    row = store._rows("SELECT * FROM events WHERE id=?", [event_id])[0]
    tokens = terms(row["title"]) | set(extra_terms)
    if not tokens:
        return []
    placeholders = ",".join("?" for _ in tokens)
    return store._rows(
        """SELECT e.id,e.semantic_id,e.title,count(*) AS overlap
        FROM event_terms t JOIN events e ON e.id=t.event_id
        WHERE t.term IN ("""
        + placeholders
        + """) AND e.venue<>? AND e.active
        AND e.qualification='qualified' AND e.semantic_id IS NOT NULL
        GROUP BY e.id,e.semantic_id,e.title ORDER BY overlap DESC,e.id LIMIT 20""",
        [*sorted(tokens), row["venue"]],
    )


def sync_status(store):
    with store.lock:
        return {
            "schema_version": "oddsfox-discovery/1",
            "interval_seconds": 900,
            "coverage_notes": [
                "A complete scan means the documented listing pagination completed without errors, not an independent audit of venue inventory.",
            ],
            "lanes": store._rows("SELECT * FROM lane_state ORDER BY lane"),
            "venues": store._rows("SELECT * FROM sync_state ORDER BY venue"),
            "counts": store._rows(
                "SELECT venue,qualification,count(*) AS count FROM events WHERE active GROUP BY venue,qualification"
            ),
            "analysis": store._rows(
                "SELECT stage,state,count(*) AS count FROM jobs WHERE stage IN ('explain','match','compare') GROUP BY stage,state"
            ),
            "categories": [
                r["category"]
                for r in store._rows(
                    "SELECT DISTINCT category FROM events WHERE active ORDER BY category"
                )
            ],
        }


def set_sync_state(store, venue, **updates):
    with store.transaction():
        row = store._rows("SELECT * FROM sync_state WHERE venue=?", [venue])[0]
        row["data"].update(updates)
        store.db.execute(
            "UPDATE sync_state SET data=? WHERE venue=?", [json.dumps(row["data"]), venue]
        )
