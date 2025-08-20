import argparse
import base64
import gzip
import io
import json
import math
import os
import random
import sys
import time
from typing import Dict, Any, Iterable, List, Optional
from datetime import datetime, timezone

import requests

import config as C


# -----------------------------
# Helpers: endpoints & requests
# -----------------------------

def export_base_url(region: str) -> str:
    region = region.upper()
    return "https://amplitude.com/api/2/export" if region == "US" else "https://analytics.eu.amplitude.com/api/2/export"

def ingest_http_v2_url(region: str) -> str:
    region = region.upper()
    return "https://api2.amplitude.com/2/httpapi" if region == "US" else "https://api.eu.amplitude.com/2/httpapi"

def batch_ingest_url(region: str) -> str:
    region = region.upper()
    return "https://api2.amplitude.com/batch" if region == "US" else "https://api.eu.amplitude.com/batch"

def basic_auth_header(api_key: str, secret: str) -> Dict[str, str]:
    token = base64.b64encode(f"{api_key}:{secret}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

def _parse_iso_to_ms(value: str | None) -> Optional[int]:
    """
    Parse ISO-8601 timestamps like '2025-08-14T10:31:22.123Z' to milliseconds.
    Returns None if value is falsy or unparseable.
    """
    if not value or not isinstance(value, str):
        return None
    try:
        # Handle trailing 'Z'
        if value.endswith('Z'):
            value = value[:-1] + '+00:00'
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def _choose_time_ms(evt: Dict[str, Any]) -> int:
    """
    Decide the outgoing 'time' field based on C.TIME_STRATEGY and what exists in the export.
    Exported events commonly include:
      - 'time' (client event time, ms since epoch)
      - 'server_received_time' (ISO-8601)
      - 'server_upload_time'  (ISO-8601)
    """
    client_ms = evt.get("time") if isinstance(evt.get("time"), int) else None
    srv_recv_ms = _parse_iso_to_ms(evt.get("server_received_time"))
    srv_upld_ms = _parse_iso_to_ms(evt.get("server_upload_time"))

    strat = (getattr(C, "TIME_STRATEGY", "prefer_client_fallback_server_received") or "").lower()

    def now_ms() -> int:
        return int(time.time() * 1000)

    if strat == "client":
        return client_ms if client_ms is not None else now_ms()

    if strat == "server_received":
        return srv_recv_ms or client_ms or now_ms()

    if strat == "server_upload":
        return srv_upld_ms or client_ms or now_ms()

    if strat == "prefer_client_fallback_server_received":
        return client_ms or srv_recv_ms or now_ms()

    if strat == "prefer_client_fallback_server_upload":
        return client_ms or srv_upld_ms or now_ms()

    # default safety
    return client_ms or srv_recv_ms or srv_upld_ms or now_ms()


# -----------------------------
# Export: load events (from API or local gz)
# -----------------------------

def stream_export_from_api(api_key: str, secret: str, region: str, start: str, end: str) -> bytes:
    """
    Calls Amplitude Export API and returns the gzipped bytes.
    The export may contain NDJSON lines (typical). We save/stream as-is.
    """
    url = f"{export_base_url(region)}?start={start}&end={end}"
    headers = basic_auth_header(api_key, secret)
    resp = requests.get(url, headers=headers, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Export failed {resp.status_code}: {resp.text[:400]}")
    return resp.content

def iterate_ndjson_from_gz_bytes(gz_bytes: bytes) -> Iterable[Dict[str, Any]]:
    """
    Given gzipped NDJSON bytes from Export API, yield parsed JSON events line by line.
    """
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as gz:
        for raw in gz:
            line = raw.decode("utf-8").strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                # Some export archives can contain metadata lines; skip if not JSON
                continue

def iterate_ndjson_from_gz_path(path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


# -----------------------------
# Transform: filter/rename properties & events
# -----------------------------

def should_keep_event(evt: Dict[str, Any]) -> bool:
    et = evt.get("event_type")
    if C.EVENT_DENYLIST and et in C.EVENT_DENYLIST:
        return False
    if C.EVENT_ALLOWLIST and et not in C.EVENT_ALLOWLIST:
        return False
    return True

def rename_event_type(et: str) -> str:
    return C.EVENT_RENAME_MAP.get(et, et)

def filter_props_for_event(et: str, props: Dict[str, Any]) -> Dict[str, Any]:
    keep = C.EVENT_PROPERTY_KEEP.get(et, C.EVENT_PROPERTY_KEEP.get("*", ["*"]))
    if keep == ["*"]:
        out = dict(props or {})
    else:
        out = {k: props.get(k) for k in keep if k in (props or {})}
    # Per-event property renames
    rename_map = C.EVENT_PROP_RENAME_MAP.get(et, {})
    if rename_map:
        out = {rename_map.get(k, k): v for k, v in out.items()}
    return out

def transform_event(evt: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Returns a new event dict compatible with Amplitude Batch/HTTP v2,
    or None to drop it.
    """
    if not should_keep_event(evt):
        return None

    et = evt.get("event_type", "")
    et = rename_event_type(et)

    # Source export props
    props_in = evt.get("event_properties") or {}
    props_out = filter_props_for_event(et, props_in)

    # Decide identity
    user_id   = C.FORCE_USER_ID   if C.FORCE_USER_ID   is not None else evt.get("user_id")
    device_id = C.FORCE_DEVICE_ID if C.FORCE_DEVICE_ID is not None else evt.get("device_id") or "migration"

    # Compute outgoing event time based on strategy
    out_time_ms = _choose_time_ms(evt)

    # Optionally attach original times into event properties (for downstream analysis)
    if getattr(C, "ORIGINAL_TIMES_AS_PROPERTIES", True):
        # Collect originals (ms or None)
        orig_client_ms = evt.get("time") if isinstance(evt.get("time"), int) else None
        orig_srv_recv_ms = _parse_iso_to_ms(evt.get("server_received_time"))
        orig_srv_upld_ms = _parse_iso_to_ms(evt.get("server_upload_time"))

        # Store under a reserved namespaced key to avoid colliding with your business properties
        mig = {
            "orig_client_time_ms": orig_client_ms,
            "orig_server_received_ms": orig_srv_recv_ms,
            "orig_server_upload_ms": orig_srv_upld_ms,
            "time_strategy_used": getattr(C, "TIME_STRATEGY", "prefer_client_fallback_server_received"),
        }

        # Merge safely (don’t overwrite user’s existing _migration, if any)
        if "_migration" in props_out and isinstance(props_out["_migration"], dict):
            props_out["_migration"].update(mig)
        else:
            props_out["_migration"] = mig

    new_evt = {
        "event_type": et,
        "event_properties": props_out,
        "user_id": user_id,
        "device_id": device_id,
        "time": out_time_ms,  # <- This is what Amplitude will use as the event's time
    }

    # Optionally forward user_properties / groups
    if "user_properties" in evt and isinstance(evt["user_properties"], dict):
        new_evt["user_properties"] = evt["user_properties"]
    if "groups" in evt and isinstance(evt["groups"], dict):
        new_evt["groups"] = evt["groups"]

    return new_evt


# -----------------------------
# Ingest: send to destination (batch with retries)
# -----------------------------

def send_batch(events: List[Dict[str, Any]], api_key: str, region: str) -> Dict[str, Any]:
    url = batch_ingest_url(region)
    payload = {"api_key": api_key, "events": events}
    tries = 0
    while True:
        tries += 1
        resp = requests.post(url, json=payload, timeout=C.REQUEST_TIMEOUTS)
        if resp.ok:
            return resp.json()
        if resp.status_code in (408, 429, 500, 502, 503, 504) and tries < C.MAX_RETRIES:
            backoff = (C.RETRY_BACKOFF_S ** tries) + random.random()
            if C.VERBOSE:
                print(f"[batch] retry {tries}: {resp.status_code}, sleeping {backoff:.2f}s")
            time.sleep(backoff)
            continue
        raise RuntimeError(f"Batch failed {resp.status_code}: {resp.text[:400]}")


# -----------------------------
# Pipeline
# -----------------------------

def iterate_source_events() -> Iterable[Dict[str, Any]]:
    if C.LOCAL_EXPORT_GZ_PATH:
        if C.VERBOSE:
            print(f"[source] reading local gz: {C.LOCAL_EXPORT_GZ_PATH}")
        yield from iterate_ndjson_from_gz_path(C.LOCAL_EXPORT_GZ_PATH)
        return

    if not (C.EXPORT_START and C.EXPORT_END):
        raise SystemExit("Set LOCAL_EXPORT_GZ_PATH or both EXPORT_START and EXPORT_END in config.py")

    if C.VERBOSE:
        print(f"[export] calling Export API {C.SOURCE_REGION} {C.EXPORT_START} → {C.EXPORT_END}")
    gz_bytes = stream_export_from_api(
        C.SOURCE_PROJECT_API_KEY,
        C.SOURCE_PROJECT_SECRET_KEY,
        C.SOURCE_REGION,
        C.EXPORT_START,
        C.EXPORT_END,
    )
    if C.VERBOSE:
        print(f"[export] received {len(gz_bytes):,} bytes")
    yield from iterate_ndjson_from_gz_bytes(gz_bytes)

def run():
    total_in = total_kept = total_sent = 0
    buf: List[Dict[str, Any]] = []

    for evt in iterate_source_events():
        total_in += 1

        new_evt = transform_event(evt)
        if new_evt is None:
            continue
        total_kept += 1

        if C.DRY_RUN:
            # Just count/preview; don't enqueue
            if C.VERBOSE and total_kept <= 3:
                print("[preview]", json.dumps(new_evt, ensure_ascii=False))
            continue

        buf.append(new_evt)
        if len(buf) >= C.BATCH_SIZE:
            resp = send_batch(buf, C.DEST_PROJECT_API_KEY, C.DEST_REGION)
            total_sent += len(buf)
            if C.VERBOSE:
                print(f"[ingest] sent {len(buf)} (total {total_sent}) → {resp}")
            buf = []

    # flush remainder
    if not C.DRY_RUN and buf:
        resp = send_batch(buf, C.DEST_PROJECT_API_KEY, C.DEST_REGION)
        total_sent += len(buf)
        if C.VERBOSE:
            print(f"[ingest] sent {len(buf)} (total {total_sent}) → {resp}")

    print(f"\nDone. read={total_in} kept={total_kept} sent={total_sent} dry_run={C.DRY_RUN}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="override DRY_RUN to preview only")
    args = parser.parse_args()
    if args.dry_run:
        C.DRY_RUN = True
    run()
