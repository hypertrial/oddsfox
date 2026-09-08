"""Bounded, read-only venue capture. Imported text never authorizes an action."""

import re
import time
from urllib.parse import quote

import httpx

from oddsfox.http import VENUE_HOSTS, stream_get
from oddsfox.ir import strict_json
from oddsfox.store import Store

ENDPOINTS = {
    "polymarket": "https://gamma-api.polymarket.com/markets/",
    "kalshi": "https://external-api.kalshi.com/trade-api/v2/markets/",
}
MAX_BYTES = 4 * 1024 * 1024


def normalize(platform: str, raw: bytes) -> tuple[str, dict, dict[str, str], list[dict]]:
    data = strict_json(raw)
    if not isinstance(data, dict):
        raise ValueError("expected a market object")
    if platform == "kalshi":
        market = data.get("market", data)
        if not isinstance(market, dict):
            raise ValueError("expected a Kalshi market object")
        native_id = market.get("ticker")
        outcomes = ["no", "yes"] if market.get("market_type") == "binary" else []
        texts = {
            key: market[key]
            for key in (
                "title",
                "subtitle",
                "yes_sub_title",
                "no_sub_title",
                "rules_primary",
                "rules_secondary",
            )
            if isinstance(market.get(key), str)
        }
        metadata = {
            "title": market.get("title", native_id),
            "outcome_ids": outcomes,
            "group": market.get("event_ticker"),
            "lifecycle": market.get("status"),
            "native_version": None,
            "source_effective_time": market.get("updated_time"),
            "resolution_date": market.get("expected_expiration_time"),
            "observation_date": market.get("occurrence_datetime"),
            "source": None,
        }
        rules_present = bool(market.get("rules_primary"))
    elif platform == "polymarket":
        market = data
        if not isinstance(market, dict):
            raise ValueError("expected a market object")
        native_id = market.get("id")
        outcomes = market.get("outcomes", [])
        tokens = market.get("clobTokenIds", [])
        outcomes = strict_json(outcomes) if isinstance(outcomes, str) else outcomes
        tokens = strict_json(tokens) if isinstance(tokens, str) else tokens
        # Preserve native token IDs when available, with the labels kept separately.
        ids = tokens if isinstance(tokens, list) and len(tokens) == 2 else outcomes
        texts = {
            key: market[key]
            for key in ("question", "description", "resolutionSource", "rulesDisclaimer")
            if isinstance(market.get(key), str)
        }
        metadata = {
            "title": market.get("question", native_id),
            "outcome_ids": ids,
            "outcome_labels": outcomes,
            "group": market.get("conditionId"),
            "lifecycle": "closed"
            if market.get("closed")
            else "active"
            if market.get("active")
            else "unknown",
            "native_version": None,
            "source_effective_time": market.get("updatedAt"),
            "resolution_date": market.get("endDate"),
            "observation_date": None,
            "source": market.get("resolutionSource"),
        }
        rules_present = bool(market.get("description"))
    else:
        raise ValueError("unsupported venue")
    if isinstance(native_id, int) and not isinstance(native_id, bool):
        native_id = str(native_id)
    if not isinstance(native_id, str) or not native_id or len(native_id) > 256:
        raise ValueError("missing or invalid native contract ID")
    if not isinstance(metadata["outcome_ids"], list) or not all(
        isinstance(x, str) and x for x in metadata["outcome_ids"]
    ):
        raise ValueError("malformed native outcomes")
    references = [
        {
            "url": url,
            "status": "not_captured",
            "reason": "governing relevance requires review; supply a captured document",
        }
        for url in sorted(set(re.findall(r"https?://[^\s<>\"\)]+", "\n".join(texts.values()))))
    ]
    metadata["capture_status"] = "available_rules" if rules_present else "missing_rules"
    metadata["governing_fields"] = {
        k: market[k]
        for k in (
            "close_time",
            "expiration_time",
            "latest_expiration_time",
            "early_close_condition",
            "strike_type",
            "floor_strike",
            "cap_strike",
            "functional_strike",
            "custom_strike",
            "notional_value_dollars",
            "mve_selected_legs",
            "mve_collection_ticker",
            "marketType",
            "market_type",
            "gameStartTime",
            "sportsMarketTypeV2",
            "line",
        )
        if k in market
    }
    import json

    texts["structured_contract_fields"] = json.dumps(
        {
            "governing_fields": metadata["governing_fields"],
            "outcome_ids": metadata["outcome_ids"],
            "outcome_labels": metadata.get("outcome_labels"),
            "resolution_date": metadata.get("resolution_date"),
            "observation_date": metadata.get("observation_date"),
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    metadata["governing_material_review_required"] = True
    metadata["source_urls"] = [ENDPOINTS[platform] + quote(native_id, safe="")]
    return native_id, metadata, texts, references


def import_capture(
    store: Store, platform: str, raw: bytes, documents: list[dict] | None = None
) -> str:
    native_id, metadata, texts, references = normalize(platform, raw)
    for i, document in enumerate(documents or []):
        if set(document) != {"url", "text", "status"} or not isinstance(document["text"], str):
            raise ValueError("documents require url, text and status")
        if not document["url"].startswith(("https://", "http://")) or document["status"] not in {
            "captured",
            "inaccessible",
        }:
            raise ValueError("invalid document provenance")
        if len(document["text"].encode()) > MAX_BYTES:
            raise ValueError("document too large")
        if document["status"] == "captured" and not document["text"]:
            raise ValueError("captured document is empty")
        if document["status"] == "captured":
            key = f"document:{i}"
            texts[key] = document["text"]
            entry = {
                "url": document["url"],
                "status": document["status"],
                "text_key": key,
                "reason": "user-supplied captured document",
            }
        else:
            entry = {
                "url": document["url"],
                "status": document["status"],
                "reason": "user-supplied captured document",
            }
        references = [r for r in references if r["url"] != document["url"]]
        references.append(entry)
    return store.capture(
        platform, native_id, raw, texts, metadata, sorted(references, key=lambda r: r["url"])
    )


def fetch(
    store: Store, platform: str, native_ids: list[str], client: httpx.Client | None = None
) -> list[dict]:
    if platform not in ENDPOINTS or not 1 <= len(native_ids) <= 250:
        raise ValueError("choose a supported venue and 1..250 IDs")
    owned = client is None
    client = client or httpx.Client(timeout=20, follow_redirects=False, trust_env=False)
    results = []
    try:
        for native_id in dict.fromkeys(native_ids):
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", native_id):
                raise ValueError("invalid native ID")
            url = ENDPOINTS[platform] + quote(native_id, safe="")
            for attempt in range(3):
                try:
                    raw = stream_get(
                        client,
                        url,
                        allowed_hosts=VENUE_HOSTS,
                        follow_redirects=False,
                        max_bytes=MAX_BYTES,
                    )
                    actual, _, _, _ = normalize(platform, raw)
                    if actual != native_id:
                        raise ValueError("response identity differs from requested market")
                    identity = import_capture(store, platform, raw)
                    results.append(
                        {"native_id": native_id, "version_id": identity, "state": "captured"}
                    )
                    break
                except (httpx.HTTPError, ValueError) as exc:
                    retry = isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)) or (
                        isinstance(exc, httpx.HTTPStatusError)
                        and (exc.response.status_code == 429 or exc.response.status_code >= 500)
                    )
                    if retry and attempt < 2:
                        delay = 2**attempt
                        if isinstance(exc, httpx.HTTPStatusError):
                            retry_after = exc.response.headers.get("retry-after", "")
                            if retry_after.isdigit():
                                delay = min(int(retry_after), 10)
                        time.sleep(delay)
                        continue
                    diagnostic = f"{type(exc).__name__}: {str(exc)[:1000]}"
                    store.refresh_failure(platform, native_id, diagnostic)
                    results.append(
                        {"native_id": native_id, "state": "failed", "reason": diagnostic}
                    )
                    break
    finally:
        if owned:
            client.close()
    return results
