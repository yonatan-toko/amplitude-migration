# =========================
# Amplitude Migration Config
# =========================




# ---- Source (for Export API or local gz file) ----------------------------------------
from platform import node


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




# ---- User identity handling ------------------------------------------------------
USER_ID_REMAP_PATH = "path/to/user_id_remap.csv"  # e.g., "user_id_remap.csv"
DEVICE_ID_REMAP_PATH = None  # e.g., "device_id_remap.csv"
REMAP_SCOPE = "user_id"  # "user_id" or "device_id" or "both"
PRESERVE_ORIGINALS_IDS = True  # True = keep original user_id/device_id in event_properties under "_migration" key
UNMAPPED_USER_ID = "keep"  # "keep" | "drop"




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