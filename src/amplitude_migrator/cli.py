import argparse, os
from pathlib import Path
import importlib.util

from amplitude_migrator.runner import run_migration

DEFAULT_CONFIG = """\
# =========================
# Amplitude Migration Config
# =========================




# ---- Source (for Export API or local gz file) ----------------------------------------
SOURCE_PROJECT_API_KEY    = "SRC_PROJECT_API_KEY"    # used for Export API
SOURCE_PROJECT_SECRET_KEY = "SRC_PROJECT_SECRET_KEY" # used for Export API
SOURCE_REGION             = "US"  # or "EU"

# If you already have an export file, set this and leave START/END empty.
# The file should be a .json.gz of NDJSON lines from Amplitude Export API.
LOCAL_EXPORT_GZ_PATH      = ""     # e.g., "exports/2025-08-14.json.gz"

# When pulling directly from Export API, set a small window first to test.
EXPORT_START = ""  # e.g., "20250814T00"  (YYYYMMDDTHH)
EXPORT_END   = ""  # e.g., "20250814T02"




# ---- Destination --------------------------------------------------------------------
DEST_PROJECT_API_KEY = "DEST_PROJECT_API_KEY"
DEST_REGION          = "US"  # or "EU"




# ---- Event selection ----------------------------------------------------------------
# If set, only these event types will be migrated.
EVENT_ALLOWLIST = [
    # "event type 1",
    # "event type 2",
    # "event type 3",
]

# Always drop these events (takes precedence over allowlist).
EVENT_DENYLIST = [
    # "page_loaded",
]




# ---- Property controls (keep/rename) ---------------------------------------------------
# Keep properties per event type. Use "*" to mean "all events" or a list
# Keep properties per event. Use "*" to mean "all properties" or a list of keys to keep.
# You can also define a "*" event key as a default for all events.
EVENT_PROPERTY_KEEP = {
    "*": ["*"],  # keep all by default
    # "event type": ["proprty 1", "property 2", "propert 3", "property 4"],
    # "event type": ["property 1"],
}

# Optionally rename event types (e.g., to new naming conventions)
# Example: "visit_submitted": "visit_created"
EVENT_RENAME_MAP = {
    # "event type": "different_event_name",
}

# Optionally rename event property keys per event
# Example: {"visit_submitted": {"doctor": "doctorName"}}
EVENT_PROP_RENAME_MAP = {
    # "event type": {"property": "new_property_name"},
}




# ---- Time handling (what to put in the outgoing "time" field) ------------------------------
# Options:
#   "client"                  -> always use original client event time if present; else now()
#   "server_received"         -> use server_received_time from export (if present), else fallback to client, else now()
#   "server_upload"           -> use server_upload_time from export (if present), else fallback to client, else now()
#   "prefer_client_fallback_server_received"
#   "prefer_client_fallback_server_upload"
TIME_STRATEGY = "prefer_client_fallback_server_received"

# Also copy the original timestamps into event_properties under a reserved key
# so you have them available analytically in the destination project.
# They will be added under:
#   _migration: {
#       "orig_client_time_ms": <int or null>,
#       "orig_server_received_ms": <int or null>,
#       "orig_server_upload_ms": <int or null>,
#   }
ORIGINAL_TIMES_AS_PROPERTIES = True



# ---- User identity handling ------------------------------------------------------
# Keep "user_id" and/or "device_id" as-is. If you need to overwrite, set these:
FORCE_USER_ID   = None  # e.g., "migrated-user" (usually keep None)
FORCE_DEVICE_ID = None  # e.g., "migration-device"




# ---- Batching & reliability -----------------------------------------------------
BATCH_SIZE       = 500
REQUEST_TIMEOUTS = 30      # seconds
MAX_RETRIES      = 5
RETRY_BACKOFF_S  = 1.5     # exponential




# ---- MTU & cost --------------------------------------------------------
# MTU = Monthly Tracked Users
MTU_BILLING_RATE_USD = 0.00         # e.g., 0.0001 for $0.0001 per user
MTU_COUNT_STRATEGY = "union"         # "user_id" | "device_id" | "union"
EXCLUDE_NULL_IDS_IN_MTU = True       # True = ignore events with null user_id/device_id in MTU count




# ---- Safety --------------------------------------------------------------------
DRY_RUN = True   # True = transform and count only; do NOT send to destination
VERBOSE = True    # print progress
REPORTS_DIR = "migration-runs"
"""

DEFAULT_README = """\
# Amplitude Project-to-Project Migrator

This tool lets you **transfer Amplitude events from one project to another**.  
You can:

- Select which **event types** to migrate  
- **Keep or rename** event properties  
- **Rename events** if needed  
- Select the way **Time** is handled
- Preserve or override **user_id / device_id**  
- Send to **US or EU** regions  
- Run safely in **dry-run** mode before sending anything  

Works with either:
- a **local Amplitude export file** (`.json.gz`), or
- directly pulling from Amplitude’s **Export API**.

---

## 1. Setup

### Prerequisites
- Python **3.9+**
- Install dependencies:
  ```bash
  pip3 install requests

---

## 2. Confugure

### open config.py and fill the fields as commented.

### importent notes:
- Set either LOCAL_EXPORT_GZ_PATH **xor** (EXPORT_START **and** EXPORT_END).
- Region must match the project’s data center (US/EU) for both source and destination.

### Very importent (!!!):
- I made a second README file called **TIME-HANDLING**, that explains how to operate the time segments in the configuration. It's pretty long, sorry about that ^_^

---

## 3. Dry run - preview

- set **DRY_RUN = TRUE**
- ```bash
    python3 migrate.py --dry-run

### importent notes:
- Reads events (from local gz or Export API),
- Applies filters/renames,
- Prints a few transformed samples,
- Does **not** send to destination.

---

## 4. Run foreal

- set **DRY_RUN = False**
- ```bash
    python3 migrate.py

### notes:
- Streams or loads events,
- Transforms according to config.py,
- Batches and sends to destination via Amplitude Batch API,
- Retries on transient errors.

### The progress will be shown in the terminal like this:
[source] reading local gz: exports/2025-08-14.json.gz
[ingest] sent 500 (total 500) → {'code': 200, 'events_ingested': 500, ...}
...
Done. read=14237 kept=13990 sent=13990 dry_run=False

---

### How it works:

### 1. Read:
- If LOCAL_EXPORT_GZ_PATH set → read gzipped NDJSON directly.
- Else → call Export API (start/end hour window) and stream results.
### 2. Transform
- Drop events by denylist; if allowlist set, keep only those.
- Rename event types (optional).
- Filter/rename properties per event (or * default).
- Preserve time, user_id/device_id (or override).
- Forward user_properties / groups if present.
### 3. Ingest
- Batch up to BATCH_SIZE.
- POST to Batch API for destination region (US/EU).
- Retry on 408/429/5xx with exponential backoff.
### 4. Report
- Print counts: read, kept, sent.
- In dry-run, only preview a few transformed events.
"""

DEFAULT_TIME = """\
# Time Handling in Amplitude Event Migration

This migration tool preserves **event time semantics** so your destination project keeps the same chronological meaning as the source.

---

## 1. Amplitude Timestamps

When you export from Amplitude, each event can include multiple timestamps:

- **time** --> the original client event time (ms since epoch). This is what the SDK usually sends.  
- **server_received_time** --> when Amplitude’s servers first received the event.  
- **server_upload_time** --> when Amplitude ingested/stored the event.  

---

## 2. The Challenge

Amplitude’s ingestion API only accepts a single `time` field (client time).  
You **cannot set** Amplitude’s internal server-received/upload timestamps directly.  
But you may want to **preserve them** for analysis after migration.

---

## 3. Our Strategy

I introduced a config option `TIME_STRATEGY` in `config.py`:

- `"client"`  
  Always use original client time if present, else `now()`.

- `"server_received"`  
  Use `server_received_time` if present, else fallback to client, else `now()`.

- `"server_upload"`  
  Use `server_upload_time` if present, else fallback to client, else `now()`.

- `"prefer_client_fallback_server_received"` (default)  
  Prefer client time, fallback to server received.

- `"prefer_client_fallback_server_upload"`  
  Prefer client time, fallback to server upload.

In all cases, if nothing is available, the tool falls back to the current time.

---

## 4. Preserving Originals

With `ORIGINAL_TIMES_AS_PROPERTIES = True` in `config.py`, the migration will attach an extra `_migration` property block to each event:

```
"_migration": {
  "orig_client_time_ms": 1723880000000,
  "orig_server_received_ms": 1723880056789,
  "orig_server_upload_ms": 1723880060000,
  "time_strategy_used": "prefer_client_fallback_server_received"
}
```

This way, even if Amplitude’s system only recognizes one `time`, you can still analyze the original server times later.

---

## Why This Matters

- **Analysis accuracy**: If the source project relied on server timestamps, you can still query them in the destination via `_migration.*`.  
- **Debugging**: Lets you verify delays between client vs. server receipt.  
- **Flexibility**: You can rerun migration with a different `TIME_STRATEGY` without losing access to originals.  

---

## My Opinion:
- Use `"prefer_client_fallback_server_received"` as default.  
- Keep `ORIGINAL_TIMES_AS_PROPERTIES = True` unless you are sure you don’t need them.  
- For charting ease, you can flatten `_migration.orig_*` into top-level properties if desired.
"""

# Utility -------------------------------------------
def _write_if_missing(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(content, encoding="utf-8")

def _load_config_module(cfg_path: Path):
    spec = importlib.util.spec_from_file_location("cfg", str(cfg_path))
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(mod)                 # type: ignore
    return mod

# Commands ------------------------------------------
def cmd_init(args: argparse.Namespace):
    project_dir = Path.cwd() / "amplitude_migration_project"
    (project_dir / "migration_runs").mkdir(parents=True, exist_ok=True)

    _write_if_missing(project_dir / "config.py", DEFAULT_CONFIG)
    _write_if_missing(project_dir / "README.md", DEFAULT_README)
    _write_if_missing(project_dir / "TIME-HANDLING.md", DEFAULT_TIME)

    print(f"Created/checked: {project_dir}")
    print("Next step: edit config.py with your details.")
    print("   Then run:")
    print("   amp-migrate run --config amplitude_migration_project/config.py --dry-run")

def cmd_run(args: argparse.Namespace):
    cfg = _load_config_module(Path(args.config).resolve())
    settings = cfg.__dict__.copy()
    if args.dry_run:
        settings["DRY_RUN"] = True
    summary = run_migration(settings)
    print(summary.get("final_line", "Done."))

def cmd_ui(args: argparse.Namespace):
    os.environ["MIGRATION_REPORTS_DIR"] = args.reports_dir or os.getcwd()
    from amplitude_migrator.web.app import start_ui
    start_ui(host=args.host, port=args.port, reload=False)

# CLI parser ----------------------------------------
def cli():
    p = argparse.ArgumentParser(prog="amp-migrate")
    sub = p.add_subparsers(dest="cmd")

    sp = sub.add_parser("init", help="Create workspace (config.py, README.md, TIME-HANDLING.md)")
    sp.set_defaults(fn=cmd_init)

    sp = sub.add_parser("run", help="Run migration")
    sp.add_argument("--config", required=True, help="Path to config.py")
    sp.add_argument("--dry-run", action="store_true", help="Force dry run")
    sp.set_defaults(fn=cmd_run)

    sp = sub.add_parser("ui", help="Launch web dashboard")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8000)
    sp.add_argument("--reports-dir", help="Directory with run reports (defaults to CWD)")
    sp.set_defaults(fn=cmd_ui)

    args = p.parse_args()
    if hasattr(args, "fn"):
        return args.fn(args)
    p.print_help()

if __name__ == "__main__":
    cli()