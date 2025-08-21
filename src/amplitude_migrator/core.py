import base64, gzip, io, json, os, random, time
from typing import Dict, Any, Iterable, List, Optional, Tuple, Set
import requests
from .time_utils import choose_time_ms, parse_iso_to_ms
from __future__ import annotations
import csv
from pathlib import Path
from typing import Dict, Iterable, Literal, Optional

RemapScope = Literal["user_id", "device_id", "both"]

def load_id_map(path: str | Path) -> Dict[str, str]:
    """
    Load a CSV mapping file with headers: old_id,new_id
    Returns dict: {old_id: new_id}
    """
    p = Path(path).expanduser().resolve()
    mapping: Dict[str, str] = {}
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if "old_id" not in reader.fieldnames or "new_id" not in reader.fieldnames:
            raise ValueError(f"ID map CSV must have headers: old_id,new_id (got {reader.fieldnames})")
        for row in reader:
            old = (row.get("old_id") or "").strip()
            new = (row.get("new_id") or "").strip()
            if old:
                mapping[old] = new
    return mapping


def apply_id_remap(
    evt: dict,
    user_map: Optional[Dict[str, str]] = None,
    device_map: Optional[Dict[str, str]] = None,
    scope: RemapScope = "user_id",
    preserve_original_ids: bool = True,
    unmapped_policy: Literal["keep", "drop"] = "keep",
    counters: Optional[dict] = None,
) -> Optional[dict]:
    """
    Apply user_id/device_id remapping in-place (returns evt) or drop (returns None).
    - scope: which identifiers to remap
    - preserve_original_ids: if True, write originals under event_properties._migration.*
    - unmapped_policy: 'keep' (default) or 'drop' when an ID is not in the map
    - counters: dict we increment for reporting
    """
    counters = counters if counters is not None else {}

    def _bump(key: str, inc: int = 1):
        counters[key] = counters.get(key, 0) + inc

    props = evt.setdefault("event_properties", {})

    # Prepare _migration audit container (don’t overwrite if exists)
    mig = props.setdefault("_migration", {})
    touched = False
    any_unmapped_drop = False

    if scope in ("user_id", "both"):
        uid = evt.get("user_id")
        if user_map is not None:
            if uid is None:
                _bump("id_remap_user_id_missing")
            elif uid in user_map:
                if preserve_original_ids and "orig_user_id" not in mig:
                    mig["orig_user_id"] = uid
                evt["user_id"] = user_map[uid]
                _bump("events_remapped_user_id")
                touched = True
            else:
                _bump("unmapped_user_ids_seen")
                if unmapped_policy == "drop":
                    any_unmapped_drop = True

    if scope in ("device_id", "both"):
        did = evt.get("device_id")
        if device_map is not None:
            if did is None:
                _bump("id_remap_device_id_missing")
            elif did in device_map:
                if preserve_original_ids and "orig_device_id" not in mig:
                    mig["orig_device_id"] = did
                evt["device_id"] = device_map[did]
                _bump("events_remapped_device_id")
                touched = True
            else:
                _bump("unmapped_device_ids_seen")
                if unmapped_policy == "drop":
                    any_unmapped_drop = True

    if touched:
        mig["id_remap_applied"] = True

    if any_unmapped_drop:
        _bump("events_dropped_unmapped")
        return None

    return evt

# ---------- Endpoints ----------
def export_base_url(region: str) -> str:
    return "https://amplitude.com/api/2/export" if (region or "US").upper() == "US" else "https://analytics.eu.amplitude.com/api/2/export"

def batch_ingest_url(region: str) -> str:
    return "https://api2.amplitude.com/batch" if (region or "US").upper() == "US" else "https://api.eu.amplitude.com/batch"

def basic_auth_header(api_key: str, secret: str) -> Dict[str, str]:
    token = base64.b64encode(f"{api_key}:{secret}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

# ---------- Export readers ----------
def stream_export_from_api(api_key: str, secret: str, region: str, start: str, end: str, timeout: int = 120) -> bytes:
    url = f"{export_base_url(region)}?start={start}&end={end}"
    r = requests.get(url, headers=basic_auth_header(api_key, secret), timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"Export failed {r.status_code}: {r.text[:400]}")
    return r.content

def iterate_ndjson_from_gz_bytes(gz_bytes: bytes) -> Iterable[Dict[str, Any]]:
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as gz:
        for raw in gz:
            line = raw.decode("utf-8", "ignore").strip()
            if not line: 
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def iterate_ndjson_from_gz_path(path: str) -> Iterable[Dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

# ---------- Transform ----------
def should_keep_event(evt: Dict[str, Any], allow: List[str], deny: List[str]) -> bool:
    et = evt.get("event_type")
    if deny and et in deny:
        return False
    if allow and et not in allow:
        return False
    return True

def rename_event_type(et: str, rename_map: Dict[str, str]) -> str:
    return rename_map.get(et, et)

def filter_props_for_event(et: str, props: Dict[str, Any], keep_map: Dict[str, List[str]], prop_rename_map: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
    keep = keep_map.get(et, keep_map.get("*", ["*"]))
    if keep == ["*"]:
        out = dict(props or {})
    else:
        out = {k: props.get(k) for k in keep if k in (props or {})}
    rmap = prop_rename_map.get(et, {})
    if rmap:
        out = {rmap.get(k, k): v for k, v in out.items()}
    return out

def transform_event(
    evt: Dict[str, Any],
    allow: List[str],
    deny: List[str],
    rename_map: Dict[str, str],
    keep_map: Dict[str, List[str]],
    prop_rename_map: Dict[str, Dict[str, str]],
    time_strategy: str,
    original_times_as_properties: bool,
    force_user_id: Optional[str],
    force_device_id: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not should_keep_event(evt, allow, deny):
        return None

    et = rename_event_type(evt.get("event_type", ""), rename_map)
    props_in = evt.get("event_properties") or {}
    props_out = filter_props_for_event(et, props_in, keep_map, prop_rename_map)

    user_id = force_user_id if force_user_id is not None else evt.get("user_id")
    device_id = force_device_id if force_device_id is not None else (evt.get("device_id") or "migration")
    out_time_ms = choose_time_ms(evt, time_strategy)

    if original_times_as_properties:
        orig_client_ms = evt.get("time") if isinstance(evt.get("time"), int) else None
        orig_srv_recv_ms = parse_iso_to_ms(evt.get("server_received_time"))
        orig_srv_upld_ms = parse_iso_to_ms(evt.get("server_upload_time"))
        mig = {
            "orig_client_time_ms": orig_client_ms,
            "orig_server_received_ms": orig_srv_recv_ms,
            "orig_server_upload_ms": orig_srv_upld_ms,
            "time_strategy_used": time_strategy,
        }
        if "_migration" in props_out and isinstance(props_out["_migration"], dict):
            props_out["_migration"].update(mig)
        else:
            props_out["_migration"] = mig

    new_evt = {
        "event_type": et,
        "event_properties": props_out,
        "user_id": user_id,
        "device_id": device_id,
        "time": out_time_ms,
    }
    if "user_properties" in evt and isinstance(evt["user_properties"], dict):
        new_evt["user_properties"] = evt["user_properties"]
    if "groups" in evt and isinstance(evt["groups"], dict):
        new_evt["groups"] = evt["groups"]
    return new_evt

# ---------- Ingest ----------
def send_batch(events: List[Dict[str, Any]], api_key: str, region: str, timeout: int, max_retries: int, backoff: float, verbose: bool) -> Dict[str, Any]:
    url = batch_ingest_url(region)
    payload = {"api_key": api_key, "events": events}
    tries = 0
    while True:
        tries += 1
        resp = requests.post(url, json=payload, timeout=timeout)
        if resp.ok:
            return resp.json()
        if resp.status_code in (408, 429, 500, 502, 503, 504) and tries < max_retries:
            sleep_s = (backoff ** tries) + random.random()
            if verbose:
                print(f"[batch] retry {tries}: {resp.status_code}, sleeping {sleep_s:.2f}s")
            time.sleep(sleep_s)
            continue
        raise RuntimeError(f"Batch failed {resp.status_code}: {resp.text[:400]}")
