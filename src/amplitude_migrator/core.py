from __future__ import annotations
import base64, gzip, io, json, os, random, time
from typing import Dict, Any, Iterable, List, Optional, Tuple, Set
import requests
from .time_utils import choose_time_ms, parse_iso_to_ms
import csv
from pathlib import Path
from typing import Dict, Iterable, Literal, Optional
import zipfile

# Top-level fields we pass through from source events unchanged (if present)
TOP_LEVEL_PASSTHROUGH: Set[str] = {
    "app_version", "library", "platform",
    "os_name", "os_version",
    "device_brand", "device_manufacturer", "device_model", "device_type",
    "carrier", "country", "region", "city", "dma", "language",
    "price", "quantity", "revenue", "productId", "revenueType",
    "location_lat", "location_lng", "ip",
    "idfa", "idfv", "adid", "android_id",
    "event_id", "session_id", "insert_id",
    "group_properties", "groups", "user_properties",
}

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

    touched = False
    any_unmapped_drop = False


    
    if scope in ("user_id", "both"):
        uid = evt.get("user_id")
        if user_map is not None:
            if uid is None:
                _bump("id_remap_user_id_missing")
            elif uid in user_map:
                #if preserve_original_ids and uid != user_map[uid]:
                #    evt.setdefault("event_properties", {}).setdefault("_migration", {})["orig_user_id"] = uid
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
                #if preserve_original_ids and did != device_map[did]:
                #    evt.setdefault("event_properties", {}).setdefault("_migration", {})["orig_device_id"] = did
                evt["device_id"] = device_map[did]
                _bump("events_remapped_device_id")
                touched = True
            else:
                _bump("unmapped_device_ids_seen")
                if unmapped_policy == "drop":
                    any_unmapped_drop = True
    
                    

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

# --- Helper: auto-detect and iterate NDJSON from bytes (ZIP/GZ/PLAIN) ---
def iterate_ndjson_from_bytes(raw_bytes: bytes) -> Iterable[Dict[str, Any]]:
    """Auto-detect and iterate events from raw export bytes.
    Handles ZIP (with .json or .json.gz entries), GZIP (.json.gz), or plain NDJSON."""
    # ZIP file? (starts with PK)
    if raw_bytes[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            for name in zf.namelist():
                if name.endswith(".json.gz"):
                    with zf.open(name) as member:
                        for evt in iterate_ndjson_from_gz_bytes(member.read()):
                            yield evt
                elif name.endswith(".json"):
                    with zf.open(name) as member:
                        for line in io.TextIOWrapper(member, encoding="utf-8", errors="ignore"):
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                yield json.loads(line)
                            except json.JSONDecodeError:
                                continue
    # GZIP file? (magic 1F 8B)
    elif raw_bytes[:2] == b"\x1f\x8b":
        yield from iterate_ndjson_from_gz_bytes(raw_bytes)
    else:
        # Assume plain NDJSON
        for line in raw_bytes.decode("utf-8", "ignore").splitlines():
            line = line.strip()
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

def iterate_ndjson_from_zip_bytes(zip_bytes: bytes) -> Iterable[Dict[str, Any]]:
    """Iterate all NDJSON events from a ZIP containing *.json.gz or *.json files."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Prefer *.json.gz entries first, then *.json
        names = sorted(zf.namelist())
        gz_names = [n for n in names if n.lower().endswith(".json.gz")]
        json_names = [n for n in names if n.lower().endswith(".json")]
        # Read gzipped JSON lines
        for name in gz_names:
            with zf.open(name, "r") as fp:
                with gzip.GzipFile(fileobj=io.BytesIO(fp.read()), mode="rb") as gz:
                    for raw in gz:
                        line = raw.decode("utf-8", "ignore").strip()
                        if not line:
                            continue
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            continue
        # Read plain JSON lines
        for name in json_names:
            with zf.open(name, "r") as fp:
                for raw in fp:
                    line = raw.decode("utf-8", "ignore").strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue

def iterate_ndjson_from_any_bytes(blob: bytes) -> Iterable[Dict[str, Any]]:
    """Auto-detect ZIP vs GZIP vs plain NDJSON and iterate events."""
    if not blob:
        return
    header = blob[:2]
    if header == b"\x1f\x8b":  # gzip
        yield from iterate_ndjson_from_gz_bytes(blob)
        return
    if header == b"PK":        # zip
        yield from iterate_ndjson_from_zip_bytes(blob)
        return
    # Fallback: treat as plain NDJSON
    for line in io.BytesIO(blob).read().decode("utf-8", "ignore").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            yield json.loads(s)
        except json.JSONDecodeError:
            continue

#
# ---------- Time helpers for minute-level windows ----------
def _compact_to_iso_utc(s: str) -> Optional[str]:
    """Convert 'YYYYMMDDTHHMM' to ISO8601 UTC 'YYYY-MM-DDTHH:MM:00Z'.
    Returns None if format doesn't match exactly.
    """
    if not isinstance(s, str):
        return None
    # expected length 13, with a literal 'T' at position 8
    if len(s) != 13 or s[8] != 'T':
        return None
    yyyy, mm, dd = s[0:4], s[4:6], s[6:8]
    hh, mi = s[9:11], s[11:13]
    # Basic sanity checks
    if not (yyyy.isdigit() and mm.isdigit() and dd.isdigit() and hh.isdigit() and mi.isdigit()):
        return None
    return f"{yyyy}-{mm}-{dd}T{hh}:{mi}:00Z"

def parse_compact_minutes_to_ms(s: Optional[str]) -> Optional[int]:
    """Parse 'YYYYMMDDTHHMM' (UTC) to epoch milliseconds. Returns None on failure."""
    if not s:
        return None
    iso = _compact_to_iso_utc(s)
    if not iso:
        return None
    try:
        # Reuse the existing ISO parser from time_utils
        return parse_iso_to_ms(iso)
    except Exception:
        return None

def export_hour_from_compact(s: Optional[str]) -> Optional[str]:
    """Derive Amplitude export hour string ('YYYYMMDDTHH') from 'YYYYMMDDTHHMM'.
    Returns None on failure.
    """
    if not s or not isinstance(s, str) or len(s) < 11:
        return None
    # Keep up to HH (index 10 exclusive -> 0..9 plus the 'H' at 10)
    return s[0:11]

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


def _get_by_path(evt: Dict[str, Any], path: Optional[str]):
    """Resolve dotted paths like 'event_properties.foo', 'user_properties.bar', or top-level 'device_id'.
    Returns None if not found or path is falsy.
    """
    if not path:
        return None
    parts = str(path).split(".")
    cur: Any = evt
    for i, p in enumerate(parts):
        if i == 0 and p in ("event_properties", "user_properties"):
            cur = evt.get(p) or {}
            continue
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return None
    return cur

# --- Helper: evaluate a simple expression string using `value` as the variable ---
# Only arithmetic and comparisons on `value` are expected. Builtins are disabled.
def _apply_expr(expr: Any, value: Any) -> Any:
    try:
        if not isinstance(expr, str):
            return value
        return eval(expr, {"__builtins__": {}}, {"value": value})
    except Exception:
        # On any failure, keep the original value
        return value

def _match_conditions(evt: Dict[str, Any], conditions: Dict[str, Any]) -> bool:
    """Return True if all dotted or top-level paths satisfy their expected values.
    Supports either plain equality or operator dicts like:
      {"not": X}, {"in": [..]}, {"not_in": [..]}, {"exists": True/False}, {"empty": True/False}
    - For dotted paths (e.g. 'event_properties.foo'), values are resolved via _get_by_path.
    - "empty" checks treat None, "", and [] as empty.
    """
    if not isinstance(conditions, dict):
        return False

    def _is_empty(v: Any) -> bool:
        return v is None or v == "" or v == []

    for path, expected in conditions.items():
        # Resolve value from event
        if isinstance(path, str) and "." in path:
            val = _get_by_path(evt, path)
        else:
            val = evt.get(path)

        # Plain equality
        if not isinstance(expected, dict):
            if val != expected:
                return False
            continue

        # Operator form
        for op, cmp in expected.items():
            try:
                if op == "not":
                    if val == cmp:
                        return False
                elif op == "in":
                    if cmp is None:
                        return False
                    container = set(cmp) if not isinstance(cmp, set) else cmp
                    if val not in container:
                        return False
                elif op == "not_in":
                    if cmp is None:
                        # nothing is disallowed -> always passes
                        continue
                    container = set(cmp) if not isinstance(cmp, set) else cmp
                    if val in container:
                        return False
                elif op == "exists":
                    want = bool(cmp)
                    exists = val is not None
                    if exists != want:
                        return False
                elif op == "empty":
                    want_empty = bool(cmp)
                    is_empty = _is_empty(val)
                    if is_empty != want_empty:
                        return False
                elif op == "range":
                    # cmp expected to be [min, max]
                    try:
                        lo, hi = cmp
                        if val is None:
                            return False
                        num = float(val)
                        if num < lo or num > hi:
                            return False
                    except Exception:
                        return False
                elif op == "contains":
                    if val is None or cmp is None:
                        return False
                    s = str(val).lower()
                    # cmp can be a single string or a list of strings
                    if isinstance(cmp, (list, tuple, set)):
                        if not any(str(word).lower() in s for word in cmp):
                            return False
                    else:
                        if str(cmp).lower() not in s:
                            return False
                elif op == "not_contains":
                    if val is None or cmp is None:
                        # if there's no value, we treat it as "does not contain" and let it pass
                        continue
                    s = str(val).lower()
                    if isinstance(cmp, (list, tuple, set)):
                        if any(str(word).lower() in s for word in cmp):
                            return False
                    else:
                        if str(cmp).lower() in s:
                            return False
                else:
                    # Unknown operator: fail safe by treating as mismatch
                    return False
            except Exception:
                return False

    return True

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
    fallback_user_properties: Optional[Dict[str, Any]] = None,
    const_props: Optional[Dict[str, Any]] = None,
    derived_props: Optional[Dict[str, Any]] = None,
    rename_rules: Optional[List[Dict[str, Any]]] = None,
    time_window_ms: Optional[Tuple[int, int]] = None,
    prop_deny_map: Optional[Dict[str, List[str]]] = None,
) -> Optional[Dict[str, Any]]:
    if not should_keep_event(evt, allow, deny):
        return None

    original_et = evt.get("event_type", "")
    et = rename_event_type(original_et, rename_map)

    # Apply conditional rename rules (first match wins)
    if isinstance(rename_rules, list) and rename_rules:
        for rule in rename_rules:
            try:
                when = rule.get("when", {})
                to = rule.get("rename_to")
                if to and _match_conditions(evt, when):
                    et = to
                    break
            except Exception:
                # Ignore malformed rule and continue
                continue

    # Special case: for 100ms_session_duration_v1.3 we only want events that matched a rename rule
    if original_et == "100ms_session_duration_v1.3" and et == original_et:
        return None

    # Filter event_properties according to keep & rename rules
    props_in = evt.get("event_properties") or {}
    props_out = filter_props_for_event(et, props_in, keep_map, prop_rename_map)

    # --- Augment event_properties with constants ---
    if isinstance(const_props, dict) and const_props:
        merged_consts: Dict[str, Any] = {}
        # Legacy flat format: {"k":"v"}
        for k, v in const_props.items():
            if k not in ("*", et) and not isinstance(v, dict):
                merged_consts[k] = v
        # Global scoped: {"*": {...}}
        gconst = const_props.get("*")
        if isinstance(gconst, dict):
            merged_consts.update(gconst)
        # Event-scoped: {"event_type": {...}}
        econst = const_props.get(et)
        if isinstance(econst, dict):
            merged_consts.update(econst)
        # Apply
        for k, v in merged_consts.items():
            props_out[k] = v

    # --- Augment event_properties with derived values ---
    if isinstance(derived_props, dict) and derived_props:
        # Build an ordered set of rules: legacy-flat -> global(*) -> event(et).
        derived_to_apply: Dict[str, Dict[str, Any]] = {}
        for k, v in derived_props.items():
            # Legacy flat rule: { new_key: {from, map, default} }
            if k not in ("*", et) and isinstance(v, dict) and (
                "from" in v or "map" in v or "default" in v
            ):
                derived_to_apply[k] = v
        gder = derived_props.get("*")
        if isinstance(gder, dict):
            for k, v in gder.items():
                if isinstance(v, dict):
                    derived_to_apply[k] = v
        eder = derived_props.get(et)
        if isinstance(eder, dict):
            for k, v in eder.items():
                if isinstance(v, dict):
                    derived_to_apply[k] = v  # event-scoped overrides

        for new_key, rule in derived_to_apply.items():
            src = rule.get("from")
            val = _get_by_path(evt, src) if src else None

            # 1) Optional mapping first (e.g., {"empty": False})
            mapped = False
            mapping = rule.get("map")
            if isinstance(mapping, dict) and (val in mapping):
                val = mapping.get(val)
                mapped = True

            # 2) Optional coercion (only if not explicitly mapped)
            if not mapped:
                coerce = rule.get("coerce")
                if coerce:
                    try:
                        if coerce == "int":
                            if val is None or (isinstance(val, str) and not val.strip()):
                                raise ValueError("empty")
                            # robust for "123" and 123.0
                            val = int(float(val))
                        elif coerce == "float":
                            if val is None or (isinstance(val, str) and not val.strip()):
                                raise ValueError("empty")
                            val = float(val)
                        elif coerce == "bool":
                            if isinstance(val, str):
                                val = val.strip().lower() in ("1", "true", "yes", "y")
                            else:
                                val = bool(val)
                        elif coerce == "str":
                            val = "" if val is None else str(val)
                        # unknown coercions are ignored
                    except Exception:
                        if "default" in rule:
                            val = rule.get("default")
                        else:
                            val = None

            # 2.5) Optional expression (only if not mapped and val is not None)
            if not mapped:
                expr = rule.get("expr")
                if expr is not None and val is not None:
                    val = _apply_expr(expr, val)

            # 3) Default if still None and default provided
            if val is None and "default" in rule:
                val = rule.get("default")

            # Write even if None (explicit null), to allow clearing
            props_out[new_key] = val

    # Respect incoming identifiers unless force_* overrides are provided
    user_id = force_user_id if force_user_id is not None else evt.get("user_id")
    device_id = force_device_id if force_device_id is not None else evt.get("device_id")

    # --- Always use the original timestamp from the source event ---
    # Prefer explicit "time" if present; otherwise prefer server_received or server_upload
    # but NEVER fallback to now(). If no timestamp exists, drop the event.
    orig_time = (
        evt.get("time")
        or evt.get("server_received_time")
        or evt.get("server_upload_time")
    )

    if orig_time is None:
        # If the event has no real timestamp, we drop it instead of inserting "now"
        return None

    out_time_ms = int(orig_time)

    # Optional minute-level time window filter (inclusive start, exclusive end)
    if time_window_ms is not None:
        try:
            start_ms, end_ms = time_window_ms
            if out_time_ms is None or out_time_ms < int(start_ms) or out_time_ms >= int(end_ms):
                return None
        except Exception:
            # If the window is malformed, fail open (do not filter)
            pass

    # Decide user_properties: prefer inline; else fallback snapshot if provided
    raw_user_props = evt.get("user_properties")
    if isinstance(raw_user_props, dict) and raw_user_props:
        out_user_props = raw_user_props
    elif isinstance(fallback_user_properties, dict) and fallback_user_properties:
        out_user_props = fallback_user_properties
    else:
        out_user_props = None

    # Start from pass-through of known top-level fields
    new_evt: Dict[str, Any] = {}
    for k in TOP_LEVEL_PASSTHROUGH:
        if k in evt:
            new_evt[k] = evt[k]

    # Override core fields we control
    new_evt["event_type"] = et
    new_evt["event_properties"] = props_out
    new_evt["user_id"] = user_id
    new_evt["device_id"] = device_id
    new_evt["time"] = out_time_ms

    if out_user_props is not None:
        new_evt["user_properties"] = out_user_props

    # --- Apply property KEEP rules to both event_properties and user_properties ---
    allowed_keys: List[str] = []
    if isinstance(keep_map, dict):
        if et in keep_map:
            allowed_keys = keep_map.get(et) or []
        elif "*" in keep_map:
            allowed_keys = keep_map.get("*") or []
    if allowed_keys and allowed_keys != ["*"]:
        if isinstance(new_evt.get("event_properties"), dict):
            new_evt["event_properties"] = {k: v for k, v in new_evt["event_properties"].items() if k in allowed_keys}
        if isinstance(new_evt.get("user_properties"), dict):
            new_evt["user_properties"] = {k: v for k, v in new_evt["user_properties"].items() if k in allowed_keys}

    # --- Apply property DENY rules (drop keys) on both namespaces ---
    deny_keys: List[str] = []
    if isinstance(prop_deny_map, dict) and prop_deny_map:
        if et in prop_deny_map:
            deny_keys.extend(prop_deny_map.get(et) or [])
        if "*" in prop_deny_map:
            deny_keys.extend(prop_deny_map.get("*") or [])
    if deny_keys:
        if isinstance(new_evt.get("event_properties"), dict):
            for k in deny_keys:
                new_evt["event_properties"].pop(k, None)
        if isinstance(new_evt.get("user_properties"), dict):
            for k in deny_keys:
                new_evt["user_properties"].pop(k, None)

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
