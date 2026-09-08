"""Three explicit public discovery adapters and exact, provenance-bearing event volume."""

import json
import re
import time
from decimal import Decimal, InvalidOperation, localcontext
from urllib.parse import quote

import httpx

from oddsfox.documents import capture_document
from oddsfox.ingest import ENDPOINTS, normalize
from oddsfox.ir import fingerprint
from oddsfox.store import now, volume_order_key

VENUES = {
    "kalshi": "Kalshi",
    "polymarket": "Polymarket International",
    "polymarket_us": "Polymarket US",
}
BASES = {
    "kalshi": "https://external-api.kalshi.com/trade-api/v2",
    "polymarket": "https://gamma-api.polymarket.com",
    "polymarket_us": "https://gateway.polymarket.us/v1",
}
SCHEMA = "oddsfox-events/1"
THRESHOLD = Decimal("100000")


def decimal(value):
    if value is None or isinstance(value, bool) or len(str(value)) > 256:
        return None
    try:
        result = Decimal(str(value))
        if not result.is_finite() or result < 0 or result >= Decimal("1e26"):
            return None
        exponent = result.as_tuple().exponent
        if not isinstance(exponent, int) or exponent < -100 or len(result.as_tuple().digits) > 128:
            return None
        return result
    except InvalidOperation, ValueError:
        return None


def native_id(value, venue):
    result = value.get("event_ticker" if venue == "kalshi" else "id")
    if isinstance(result, int) and not isinstance(result, bool):
        result = str(result)
    if not isinstance(result, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", result):
        raise ValueError("invalid native event identity")
    return result


def market_id(value, venue):
    return str(value.get("ticker" if venue == "kalshi" else "id", ""))


def tradable(market, venue):
    if venue == "kalshi":
        return market.get("status") == "active"
    if not market.get("active") or market.get("closed") or market.get("archived"):
        return False
    if venue == "polymarket":
        return market.get("acceptingOrders") is True
    return market.get("status") == "MARKET_STATUS_OPEN" or market.get("ep3Status") == "OPEN"


def volume(event, markets, venue, complete=True):
    with localcontext() as context:
        context.prec = 256
        return _volume(event, markets, venue, complete)


def _volume(event, markets, venue, complete):
    basis = "USD face-value notional" if venue == "kalshi" else "venue-reported USD turnover"
    total = None
    source = "child market lifetime volumes"
    if venue != "kalshi":
        total = decimal(event.get("volume"))
        if total is not None:
            source = "event.volume"
    if total is None and complete and markets:
        values = []
        for m in markets:
            if venue == "kalshi":
                count, face = decimal(m.get("volume_fp")), decimal(m.get("notional_value_dollars"))
                v = count * face if count is not None and face is not None and face > 0 else None
            else:
                v = decimal(m.get("volume", m.get("volumeNum")))
            values.append(v)
        if all(v is not None for v in values):
            total = sum((v for v in values if v is not None), Decimal(0))
    if total is not None and total >= Decimal("1e26"):
        total = None
    return {
        "amount": str(total) if total is not None else None,
        "currency": "USD",
        "window": "lifetime",
        "basis": basis,
        "source": source,
        "complete": total is not None,
        "observed_at": now(),
        "reason": ""
        if total is not None
        else "Lifetime volume or complete child membership is unavailable",
    }


def request_json(client, url, params=None):
    for attempt in range(3):
        try:
            with client.stream("GET", url, params=params, timeout=20) as response:
                response.raise_for_status()
                chunks, size, start = [], 0, time.monotonic()
                for chunk in response.iter_bytes():
                    size += len(chunk)
                    if size > 16 * 1024 * 1024 or time.monotonic() - start > 45:
                        raise ValueError("discovery response exceeds resource budget")
                    chunks.append(chunk)
            raw = b"".join(chunks)

            def unique(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError("duplicate JSON key")
                    result[key] = value
                return result

            data = json.loads(
                raw,
                parse_float=str,
                object_pairs_hook=unique,
                parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")),
            )
            if not isinstance(data, (dict, list)):
                raise ValueError("expected discovery object or list")
            return data, raw
        except (httpx.HTTPError, ValueError) as exc:
            retry = isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)) or (
                isinstance(exc, httpx.HTTPStatusError)
                and (exc.response.status_code == 429 or exc.response.status_code >= 500)
            )
            if not retry or attempt == 2:
                raise
            delay = 2**attempt
            if isinstance(exc, httpx.HTTPStatusError):
                value = exc.response.headers.get("retry-after", "")
                if value.isdigit():
                    delay = min(int(value), 10)
            time.sleep(delay)
    raise AssertionError("unreachable")


def pages(client, venue, checkpoint=None):
    """Yield one page and its resume cursor. No total-event limit or volume prefilter."""
    cp = checkpoint or {"stream": 0, "cursor": "", "offset": 0}
    streams = (
        ["/events", "/events/multivariate"]
        if venue == "kalshi"
        else ["/events/keyset" if venue == "polymarket" else "/events"]
    )
    for stream in range(int(cp.get("stream", 0)), len(streams)):
        cursor = cp.get("cursor", "") if stream == cp.get("stream", 0) else ""
        offset = int(cp.get("offset", 0)) if stream == cp.get("stream", 0) else 0
        seen = set()
        while True:
            params = {"limit": 100}
            if venue == "kalshi":
                params["with_nested_markets"] = "true"
                if stream == 0:
                    params["status"] = "open"
                if cursor:
                    params["cursor"] = cursor
            else:
                params.update({"closed": "false"})
                if venue == "polymarket_us":
                    params.update(
                        {
                            "active": "true",
                            "archived": "false",
                            "offset": offset,
                            "orderBy": "id",
                            "orderDirection": "asc",
                        }
                    )
                elif cursor:
                    params["after_cursor"] = cursor
            url = BASES[venue] + streams[stream]
            data, raw = request_json(client, url, params)
            events = data.get("events") if isinstance(data, dict) else data
            if not isinstance(events, list) or not all(isinstance(e, dict) for e in events):
                raise ValueError("invalid event page")
            if venue == "polymarket_us":
                following = str(offset + len(events)) if len(events) == 100 else ""
            else:
                following = data.get("cursor" if venue == "kalshi" else "next_cursor", "") or ""
                if not isinstance(following, str):
                    raise ValueError("invalid pagination cursor")
            signature = fingerprint([native_id(e, venue) for e in events])
            if (following and following in seen) or (events and signature in seen):
                raise ValueError("repeated discovery page/cursor; scan incomplete")
            seen.update({following, signature})
            next_cp = (
                {"stream": stream, "cursor": following, "offset": offset + len(events)}
                if following
                else {"stream": stream + 1, "cursor": "", "offset": 0}
            )
            yield events, raw, url, next_cp
            if not following:
                break
            cursor, offset = following, offset + len(events)


def complete_markets(client, event, venue, store=None):
    sources = []
    markets = event.get("markets", [])
    if not isinstance(markets, list) or not all(isinstance(m, dict) for m in markets):
        raise ValueError("invalid child markets")
    unique = {market_id(m, venue): m for m in markets}
    if "" in unique:
        raise ValueError("missing child market ID")
    complete = True
    # Kalshi's nested event response explicitly omits old settled markets. Retrieve
    # both tiers by event identity; an unavailable historical tier makes the total unknown.
    if venue == "kalshi":
        for endpoint in ("/markets", "/historical/markets"):
            cursor, seen = "", set()
            try:
                while True:
                    data, membership_raw = request_json(
                        client,
                        BASES[venue] + endpoint,
                        {"event_ticker": native_id(event, venue), "limit": 200, "cursor": cursor},
                    )
                    if store is not None:
                        sources.append(
                            {
                                "raw_artifact": store.put_artifact(membership_raw),
                                "source_url": BASES[venue] + endpoint,
                            }
                        )
                    rows = data.get("markets")
                    if not isinstance(rows, list):
                        raise ValueError("missing market membership")
                    for m in rows:
                        if not isinstance(m, dict) or m.get("event_ticker") != native_id(
                            event, venue
                        ):
                            raise ValueError("membership response contains another event")
                        unique[market_id(m, venue)] = m
                    cursor = data.get("cursor") or ""
                    if not cursor:
                        break
                    if cursor in seen:
                        raise ValueError("repeated membership cursor")
                    seen.add(cursor)
            except httpx.HTTPError, ValueError:
                complete = False
    if venue == "polymarket_us":
        counts = event.get("marketCounts")
        if isinstance(counts, dict):
            expected = counts.get("total")
            if isinstance(expected, int) and expected > len(unique):
                complete = False
    return list(unique.values()), complete, sources


def capture_market(
    store,
    client,
    venue,
    market,
    shared_documents=(),
    document_cache=None,
    *,
    event_context=None,
    event_page_artifact=None,
):
    expected = market_id(market, venue)
    _, raw = request_json(client, ENDPOINTS[venue] + quote(expected, safe=""))
    identity, metadata, texts, references = normalize(venue, raw)
    if identity != expected:
        raise ValueError("market detail identity does not match requested child")
    if event_context is not None:
        texts["event_rules"] = json.dumps(event_context, sort_keys=True, ensure_ascii=False)
        metadata["event_page_artifact"] = event_page_artifact
        if event_context.get("mve_collection_ticker") or event_context.get("mve_selected_legs"):
            metadata["governing_fields"]["is_combination"] = True
        shared_documents = set(shared_documents) | set(
            re.findall(
                r'https?://[^\s<>"\)]+',
                "\n".join(value for value in event_context.values() if isinstance(value, str)),
            )
        )
    urls = {r["url"] for r in references} | set(shared_documents)
    document_cache = {} if document_cache is None else document_cache
    for index, url in enumerate(sorted(urls)):
        if url not in document_cache:
            document_cache[url] = capture_document(store, url)
        document = dict(document_cache[url])
        key = f"document:{index}"
        if document["status"] == "captured":
            texts[key] = document.pop("text")
            document["text_key"] = key
        else:
            document.pop("text", None)
        references = [r for r in references if r["url"] != url]
        references.append(document)
    return store.capture(
        venue, identity, raw, texts, metadata, sorted(references, key=lambda r: r["url"])
    )


def terms(text):
    return set(re.findall(r"[\w]+", text.lower())) - {
        "the",
        "will",
        "a",
        "an",
        "to",
        "of",
        "in",
        "on",
        "by",
        "be",
        "and",
        "or",
        "is",
    }


def save_event(store, client, venue, event, run_id, page_artifact, source_url, document_cache=None):
    eid = native_id(event, venue)
    key = f"{venue}:{eid}"
    markets, complete, membership_sources = complete_markets(client, event, venue, store)
    active = [m for m in markets if tradable(m, venue)]
    if venue != "kalshi" and (
        event.get("closed") or event.get("archived") or event.get("active") is False
    ):
        active = []
    if not complete:
        previous = store._rows("SELECT * FROM events WHERE id=?", [key])
        if previous:
            retained = previous[0]["data"] | {
                "latest_attempt": now(),
                "stale_reason": "Child membership retrieval incomplete; last successful event retained",
            }
            with store.transaction():
                store.db.execute(
                    "UPDATE events SET data=? WHERE id=?",
                    [json.dumps(retained), key],
                )
            raise ValueError("Incomplete child membership; last successful event retained")

    amount = volume(event, markets, venue, complete)
    qualification = (
        "unknown"
        if amount["amount"] is None
        else "qualified"
        if Decimal(amount["amount"]) > THRESHOLD
        else "below_threshold"
    )
    contracts = []
    documents = []
    if venue == "kalshi" and event.get("series_ticker") and active and qualification == "qualified":
        series_id = str(event["series_ticker"])
        data, series_raw = request_json(
            client, BASES[venue] + "/series/" + quote(series_id, safe="")
        )
        series = data.get("series", {})
        documents = [
            series[k]
            for k in ("contract_url", "contract_terms_url")
            if isinstance(series.get(k), str)
        ]
        # Retain the exact series response that identifies governing documents.
        series_artifact = store.put_artifact(series_raw)
    else:
        series_artifact = None
    if venue == "polymarket_us" and active and qualification == "qualified":
        documents.append("https://polymarketexchange.com/files/legal/latest/rulebook")
    event_context = {
        "native_event_id": eid,
        **{
            field: event[field]
            for field in (
                "title",
                "description",
                "resolutionSource",
                "mve_collection_ticker",
                "mve_selected_legs",
            )
            if field in event
        },
    }
    if active and qualification == "qualified":
        for market in active:
            contracts.append(
                capture_market(
                    store,
                    client,
                    venue,
                    market,
                    documents,
                    document_cache,
                    event_context=event_context,
                    event_page_artifact=page_artifact,
                )
            )
    semantic = None
    semantic_data = {
        "schema_version": SCHEMA,
        "venue": venue,
        "native_id": eid,
        "title": event.get("title", eid),
        "description": event.get("description", ""),
        "contracts": sorted(contracts),
        "combination": bool(
            event.get("mve_collection_ticker") or any(m.get("mve_selected_legs") for m in active)
        ),
        "legs": [m.get("mve_selected_legs", []) for m in active if m.get("mve_selected_legs")],
    }
    semantic_data["text_artifacts"] = {
        "event_rules": store.put_artifact(
            json.dumps(
                {
                    "title": semantic_data["title"],
                    "description": semantic_data["description"],
                    "legs": semantic_data["legs"],
                    "combination": semantic_data["combination"],
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode()
        )
    }
    data = {
        "schema_version": SCHEMA,
        "volume": amount,
        "active_markets": [market_id(m, venue) for m in active],
        "membership_complete": complete,
        "market_count": len(markets),
        "contracts": contracts,
        "page_artifact": page_artifact,
        "source_url": source_url,
        "combination": semantic_data["combination"],
        "series_artifact": series_artifact,
        "membership_sources": membership_sources,
    }
    with store.transaction():
        if contracts:
            old = store.current("event_semantics", key)
            if (
                old
                and {
                    k: v
                    for k, v in old["data"].items()
                    if k not in {"revision", "source_page_artifact"}
                }
                == semantic_data
            ):
                semantic = old["id"]
            else:
                semantic_data["source_page_artifact"] = page_artifact
                semantic_data["revision"] = (
                    len(
                        store._rows(
                            "SELECT id FROM nodes WHERE kind='event_semantics' AND logical=?", [key]
                        )
                    )
                    + 1
                )
                semantic = store.insert(
                    "event_semantics", key, semantic_data, contracts, "CAPTURED"
                )
        store.db.execute(
            """INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
            title=excluded.title,category=excluded.category,volume=excluded.volume,qualification=excluded.qualification,
            active=excluded.active,seen_run=excluded.seen_run,semantic_id=excluded.semantic_id,data=excluded.data,updated=excluded.updated""",
            [
                key,
                venue,
                eid,
                str(event.get("title", eid)),
                str(event.get("category", "Other")),
                volume_order_key(amount["amount"]),
                qualification,
                bool(active),
                run_id if complete else "",
                semantic,
                json.dumps(data),
                now(),
            ],
        )
        store.db.execute("DELETE FROM event_terms WHERE event_id=?", [key])
        indexed = terms(str(event.get("title", "")))
        explanation = store.current("explanation", semantic) if semantic else None
        if explanation:
            indexed |= terms(
                " ".join(explanation["data"]["entities"] + explanation["data"]["dates"])
            )
        for term in indexed:
            store.db.execute(
                "INSERT INTO event_terms VALUES (?,?) ON CONFLICT DO NOTHING", [term, key]
            )
    if not complete:
        raise ValueError("Incomplete child membership; scan is partial")
    return key
