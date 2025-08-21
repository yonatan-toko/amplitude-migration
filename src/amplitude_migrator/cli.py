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
# Keep properties per event type. Use "*" for all events and "*" inside the list for all properties.
# Example keeps:
#   EVENT_PROPERTY_KEEP = {
#       "*": ["*"],                      # keep all properties for all events (default)
#       # "visit_submitted": ["doctorName", "visitDate"],
#       # "purchase": ["price", "sku"]
#   }
EVENT_PROPERTY_KEEP = {
    "*": ["*"],  # keep all by default
}

# Optionally rename event types (e.g., to new naming conventions)
# Example: {"visit_submitted": "visit_created"}
EVENT_RENAME_MAP = {
}

# Optionally rename event property keys per event
# Example: {"visit_submitted": {"doctor": "doctorName"}}
EVENT_PROP_RENAME_MAP = {
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
REPORTS_DIR = "migration_runs"
"""

DEFAULT_README = """\
# Amplitude Project-to-Project Migrator

This tool lets you **transfer Amplitude events from one project to another**.

You can:
- Select which **event types** to migrate
- **Keep or rename** event properties
- **Rename events** if needed
- Choose how **time** is handled and preserve originals
- Preserve or override **user_id / device_id**
- Send to **US or EU** regions
- Run safely in **dry-run** mode before sending anything

Works with either:
- a **local Amplitude export file** (`.json.gz`), or
- directly pulling from Amplitude’s **Export API**.

---

## 1) Setup

### Prerequisites
- Python **3.9+**
- Install dependencies:

```bash
pip3 install requests
```

---

## 2) Configure

Open `config.py` and fill the fields as commented.

> Notes
> - Set either `LOCAL_EXPORT_GZ_PATH` **xor** (`EXPORT_START` **and** `EXPORT_END`).
> - Region must match the project’s data center (US/EU) for both source and destination.
> - See `TIME-HANDLING.md` for details on timestamp strategies.

---

## 3) Dry run — preview

Set `DRY_RUN = True` in `config.py`, then run:

```bash
amp-migrate run --config amplitude_migration_project/config.py --dry-run
```

This will:
- Read events (from local gz or Export API)
- Apply filters/renames
- Print a few transformed samples
- **Not** send anything to the destination

---

## 4) Real run

Set `DRY_RUN = False` in `config.py`, then run:

```bash
amp-migrate run --config amplitude_migration_project/config.py
```

You should see progress like:
```
[source] reading local gz: exports/2025-08-14.json.gz
[ingest] sent 500 (total 500) → {"code": 200, "events_ingested": 500, ...}
...
Done. read=14237 kept=13990 sent=13990 dry_run=False
```

---

## 5) UI — view reports and sample events

Launch the dashboard:

```bash
amp-migrate ui --port 8010
```

Open the printed URL and click a run to view:
- Summary & MTU (with estimated cost)
- ID Remap settings (if used)
- **Samples → Events** — full JSON of captured events (properties & values)

If your run reports are in a different folder, start with:
```bash
amp-migrate ui --reports-dir /path/to/migration_runs
```

---

## 6) User ID remap (optional)

Create a CSV `id_map.csv` with headers `old_id,new_id` and point to it in `config.py`:
```python
USER_ID_REMAP_PATH = "id_map.csv"
REMAP_SCOPE = "user_id"  # or "both"
```
Run a dry run first, then a real run.

"""

DEFAULT_TIME = """\
# Time Handling in Amplitude Event Migration

This migration tool preserves **event time semantics** so your destination project keeps the same chronological meaning as the source.

---

## 1) Amplitude Timestamps

From Amplitude exports, events may include:
- **time** — the original client event time (ms since epoch)
- **server_received_time** — when Amplitude received the event
- **server_upload_time** — when Amplitude ingested/stored the event

---

## 2) The Challenge

Amplitude’s ingestion API only accepts a single `time` field. You **cannot set** Amplitude’s internal server times directly. But you may want to **preserve them** for analysis after migration.

---

## 3) Strategy

Set `TIME_STRATEGY` in `config.py`:
- `"client"` — always use original client time if present, else `now()`
- `"server_received"` — use server_received_time, fallback to client, else `now()`
- `"server_upload"` — use server_upload_time, fallback to client, else `now()`
- `"prefer_client_fallback_server_received"` (default) — prefer client time, fallback to server received
- `"prefer_client_fallback_server_upload"` — prefer client time, fallback to server upload

If nothing is available, we fallback to the current time.

---

## 4) Preserve originals

With `ORIGINAL_TIMES_AS_PROPERTIES = True`, the migration will attach an extra `_migration` block:

```
"_migration": {
  "orig_client_time_ms": 1723880000000,
  "orig_server_received_ms": 1723880056789,
  "orig_server_upload_ms": 1723880060000,
  "time_strategy_used": "prefer_client_fallback_server_received"
}
```

---

## Recommendations
- Keep `ORIGINAL_TIMES_AS_PROPERTIES = True` unless you’re sure you don’t need them.
- Default `TIME_STRATEGY = "prefer_client_fallback_server_received"` works well for most cases.
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
    print("   UI: amp-migrate ui --port 8010  # then open the printed URL")

def cmd_run(args: argparse.Namespace):
    cfg = _load_config_module(Path(args.config).resolve())
    settings = cfg.__dict__.copy()

    # Allow overriding reports directory for this run
    if getattr(args, "reports_dir", None):
        os.environ["MIGRATION_REPORTS_DIR"] = str(Path(args.reports_dir).expanduser().resolve())

    if args.dry_run:
        settings["DRY_RUN"] = True
    summary = run_migration(settings)
    print(summary.get("final_line", "Done."))

def cmd_ui(args):
    from pathlib import Path
    # If the user supplied --reports-dir, use it. Otherwise default to <CWD>/migration_runs
    default_reports = Path.cwd() / "migration_runs"
    reports_dir = Path(args.reports_dir).expanduser() if args.reports_dir else default_reports
    os.environ["MIGRATION_REPORTS_DIR"] = str(reports_dir.resolve())

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
    sp.add_argument("--reports-dir", help="Override reports directory for this run")
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