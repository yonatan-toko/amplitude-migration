import json, os, time
from pathlib import Path
from typing import Dict, Any, Iterable, List, Optional, Set

from . import core
from amplitude_migrator.core import load_id_map, apply_id_remap

def _iter_source_events(cfg) -> Iterable[Dict[str, Any]]:
    if cfg.get("LOCAL_EXPORT_GZ_PATH"):
        if cfg.get("VERBOSE"): print(f"[source] local gz: {cfg['LOCAL_EXPORT_GZ_PATH']}")
        yield from core.iterate_ndjson_from_gz_path(cfg["LOCAL_EXPORT_GZ_PATH"])
        return
    if not (cfg.get("EXPORT_START") and cfg.get("EXPORT_END")):
        raise SystemExit("Set LOCAL_EXPORT_GZ_PATH or both EXPORT_START and EXPORT_END in config.")
    if cfg.get("VERBOSE"):
        print(f"[export] {cfg['SOURCE_REGION']} {cfg['EXPORT_START']} → {cfg['EXPORT_END']}")
    gz = core.stream_export_from_api(
        cfg["SOURCE_PROJECT_API_KEY"], cfg["SOURCE_PROJECT_SECRET_KEY"],
        cfg["SOURCE_REGION"], cfg["EXPORT_START"], cfg["EXPORT_END"]
    )
    if cfg.get("VERBOSE"): print(f"[export] bytes={len(gz):,}")
    yield from core.iterate_ndjson_from_any_bytes(gz)

def _mtu_sets_add(user_ids: Set[str], device_ids: Set[str], evt: Dict[str, Any], exclude_null: bool):
    u = evt.get("user_id")
    d = evt.get("device_id")
    if u and (not exclude_null or u != "null"): user_ids.add(str(u))
    if d and (not exclude_null or d != "null"): device_ids.add(str(d))

def _mtu_estimate(user_ids: Set[str], device_ids: Set[str], strategy: str) -> int:
    s = (strategy or "union").lower()
    if s == "user_id":   return len(user_ids)
    if s == "device_id": return len(device_ids)
    return len(user_ids.union(device_ids))  # union default

def run_migration(cfg: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.time()
    total_in = total_kept = total_sent = 0
    unique_user_ids, unique_device_ids = set(), set()
    buf: List[Dict[str, Any]] = []
    batches: List[int] = []

    sample_limit = int(cfg.get("REPORT_SAMPLE_LIMIT", 20))
    sample_events: List[Dict[str, Any]] = []

    # Caches of last known user_properties from the source stream
    last_user_props_by_user_id: Dict[str, Dict[str, Any]] = {}
    last_user_props_by_device_id: Dict[str, Dict[str, Any]] = {}

    # --- Optional ID remapping config ---
    user_map_path = cfg.get("USER_ID_REMAP_PATH") or cfg.get("ID_REMAP_PATH")
    device_map_path = cfg.get("DEVICE_ID_REMAP_PATH")
    remap_scope = cfg.get("REMAP_SCOPE", "user_id")  # "user_id" | "device_id" | "both"
    preserve_original_ids = bool(cfg.get("PRESERVE_ORIGINAL_IDS", True))
    unmapped_policy = cfg.get("UNMAPPED_ID_POLICY", "keep")  # "keep" | "drop"

    user_map = load_id_map(user_map_path) if user_map_path else None
    device_map = (
        load_id_map(device_map_path) if device_map_path else
        (user_map if (user_map and remap_scope in ("device_id", "both")) else None)
    )

    remap_counters: Dict[str, int] = {}

    for evt in _iter_source_events(cfg):
        total_in += 1

        # Update caches with source-side user_properties when present
        try:
            src_up = evt.get("user_properties")
            if isinstance(src_up, dict) and src_up:
                uid = evt.get("user_id")
                did = evt.get("device_id")
                if uid:
                    last_user_props_by_user_id[str(uid)] = src_up
                if did:
                    last_user_props_by_device_id[str(did)] = src_up
        except Exception:
            pass

        # Determine fallback user_properties snapshot if this event has none
        fallback_up = None
        try:
            has_up = isinstance(evt.get("user_properties"), dict) and bool(evt.get("user_properties"))
            if not has_up:
                uid = evt.get("user_id")
                did = evt.get("device_id")
                if uid and str(uid) in last_user_props_by_user_id:
                    fallback_up = last_user_props_by_user_id[str(uid)]
                elif did and str(did) in last_user_props_by_device_id:
                    fallback_up = last_user_props_by_device_id[str(did)]
        except Exception:
            fallback_up = None

        new_evt = core.transform_event(
            evt,
            rename_rules=cfg.get("EVENT_RENAME_RULES", []),
            allow=cfg.get("EVENT_ALLOWLIST", []),
            deny=cfg.get("EVENT_DENYLIST", []),
            rename_map=cfg.get("EVENT_RENAME_MAP", {}),
            keep_map=cfg.get("EVENT_PROPERTY_KEEP", {"*": ["*"]}),
            prop_rename_map=cfg.get("EVENT_PROP_RENAME_MAP", {}),
            time_strategy=cfg.get("TIME_STRATEGY", "prefer_client_fallback_server_received"),
            original_times_as_properties=cfg.get("ORIGINAL_TIMES_AS_PROPERTIES", True),
            force_user_id=cfg.get("FORCE_USER_ID"),
            force_device_id=cfg.get("FORCE_DEVICE_ID"),
            fallback_user_properties=fallback_up,
            const_props=cfg.get("EVENT_CONST_PROPERTIES", {}),
            derived_props=cfg.get("EVENT_DERIVED_PROPERTIES", {}),
        )
        if new_evt is None:
            continue

        # Apply optional ID remapping before counting/preview/sending
        if user_map or device_map:
            new_evt = apply_id_remap(
                new_evt,
                user_map=user_map,
                device_map=device_map,
                scope=remap_scope,  # type: ignore[arg-type]
                preserve_original_ids=preserve_original_ids,
                unmapped_policy=unmapped_policy,  # type: ignore[arg-type]
                counters=remap_counters,
            )
            if new_evt is None:
                # dropped due to unmapped policy
                continue

        # Capture sample events for UI (store up to sample_limit)
        if len(sample_events) < sample_limit:
            try:
                # store a shallow copy to avoid later mutation
                sample_events.append(json.loads(json.dumps(new_evt)))
            except Exception:
                pass

        total_kept += 1

        # MTU tracking (estimate)
        _mtu_sets_add(unique_user_ids, unique_device_ids, new_evt, cfg.get("EXCLUDE_NULL_IDS_IN_MTU", True))

        if cfg.get("DRY_RUN", False):
            if cfg.get("VERBOSE", False) and total_kept <= 3:
                print("[preview]", json.dumps(new_evt, ensure_ascii=False))
            continue

        buf.append(new_evt)
        if len(buf) >= int(cfg.get("BATCH_SIZE", 500)):
            core.send_batch(buf, cfg["DEST_PROJECT_API_KEY"], cfg["DEST_REGION"],
                            timeout=int(cfg.get("REQUEST_TIMEOUTS", 30)),
                            max_retries=int(cfg.get("MAX_RETRIES", 5)),
                            backoff=float(cfg.get("RETRY_BACKOFF_S", 1.5)),
                            verbose=bool(cfg.get("VERBOSE", True)))
            total_sent += len(buf)
            batches.append(len(buf))
            if cfg.get("VERBOSE", True):
                print(f"[ingest] sent {len(buf)} (total {total_sent})")
            buf = []

    if not cfg.get("DRY_RUN", False) and buf:
        core.send_batch(buf, cfg["DEST_PROJECT_API_KEY"], cfg["DEST_REGION"],
                        timeout=int(cfg.get("REQUEST_TIMEOUTS", 30)),
                        max_retries=int(cfg.get("MAX_RETRIES", 5)),
                        backoff=float(cfg.get("RETRY_BACKOFF_S", 1.5)),
                        verbose=bool(cfg.get("VERBOSE", True)))
        total_sent += len(buf)
        batches.append(len(buf))
        if cfg.get("VERBOSE", True):
            print(f"[ingest] sent {len(buf)} (total {total_sent})")

    ended_at = time.time()

    mtu_strategy = cfg.get("MTU_COUNT_STRATEGY", "union")
    mtu_rate = float(cfg.get("MTU_BILLING_RATE_USD", 0.0))
    mtu = _mtu_estimate(unique_user_ids, unique_device_ids, mtu_strategy)
    est_cost = round(mtu * mtu_rate, 4)

    # --- Determine reports directory from settings/UI ---
    rep_dir_cfg = cfg.get("REPORTS_DIR")
    if rep_dir_cfg:
        reports_dir_path = Path(rep_dir_cfg)
        if not reports_dir_path.is_absolute():
            # resolve relative to CWD
            reports_dir_path = (Path.cwd() / reports_dir_path).resolve()
    else:
        # Stable default under the initialized project folder
        reports_dir_path = (Path.cwd() / "amplitude_migration_project" / "migration_runs").resolve()

    # Ensure directory exists
    reports_dir_path.mkdir(parents=True, exist_ok=True)

    # Build filename and absolute path
    name = time.strftime("run-%Y%m%d-%H%M%S.json", time.gmtime(ended_at))
    path_obj = reports_dir_path / name
    path = str(path_obj)
    if cfg.get("VERBOSE", True):
        print(f"[report] writing JSON to: {path}")

    summary = {
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_s": round(ended_at - started_at, 3),
        "report_path": path,
        "source": {
            "region": cfg.get("SOURCE_REGION"),
            "export_start": cfg.get("EXPORT_START"),
            "export_end": cfg.get("EXPORT_END"),
            "local_export_path": cfg.get("LOCAL_EXPORT_GZ_PATH"),
        },
        "destination": {
            "region": cfg.get("DEST_REGION"),
        },
        "counters": (
            lambda base: (base.update(remap_counters) or base)
        )({
            "events_read": total_in,
            "events_kept": total_kept,
            "events_sent": total_sent,
            "batches": batches,
        }),
        "mtu": {
            "unique_user_ids": len(unique_user_ids),
            "unique_device_ids": len(unique_device_ids),
            "strategy": mtu_strategy,
            "rate_usd": mtu_rate,
            "estimate": mtu,
            "estimated_cost_usd": est_cost,
        },
        "id_remap": {
            "enabled": bool(user_map or device_map),
            "user_map_path": str(Path(user_map_path).resolve()) if user_map_path else None,
            "device_map_path": str(Path(device_map_path).resolve()) if device_map_path else None,
            "scope": remap_scope,
            "preserve_original_ids": preserve_original_ids,
            "unmapped_policy": unmapped_policy,
        },
        "samples": {
            "limit": sample_limit,
            "count": len(sample_events),
            "events": sample_events,
        },
        "settings": {
            "dry_run": bool(cfg.get("DRY_RUN", False)),
            "batch_size": int(cfg.get("BATCH_SIZE", 500)),
            "time_strategy": cfg.get("TIME_STRATEGY", "prefer_client_fallback_server_received"),
            "original_times_as_properties": bool(cfg.get("ORIGINAL_TIMES_AS_PROPERTIES", True)),
            "allowlist": cfg.get("EVENT_ALLOWLIST", []),
            "denylist": cfg.get("EVENT_DENYLIST", []),
            "rename_map_count": len(dict(cfg.get("EVENT_RENAME_MAP", {})) or {}),
            "rename_rules_count": len(list(cfg.get("EVENT_RENAME_RULES", [])) or []),
            "const_props": (
                list((cfg.get("EVENT_CONST_PROPERTIES", {}) or {}).keys())
                if isinstance(cfg.get("EVENT_CONST_PROPERTIES", {}), dict) else []
            ),
            "derived_props": (
                list((cfg.get("EVENT_DERIVED_PROPERTIES", {}) or {}).keys())
                if isinstance(cfg.get("EVENT_DERIVED_PROPERTIES", {}), dict) else []
            ),
        },
        "augmentation_preview": {
            "EVENT_CONST_PROPERTIES": cfg.get("EVENT_CONST_PROPERTIES", {}),
            "EVENT_DERIVED_PROPERTIES": cfg.get("EVENT_DERIVED_PROPERTIES", {}),
            "EVENT_RENAME_RULES": cfg.get("EVENT_RENAME_RULES", []),
        },
    }

    # Save JSON report
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Final console line
    print(
        f"Done. read={total_in} kept={total_kept} sent={total_sent} "
        f"mtu≈{mtu} estimated_cost≈${est_cost} "
        f"(strategy={mtu_strategy}, rate=${mtu_rate}/MTU)"
    )

    return summary
