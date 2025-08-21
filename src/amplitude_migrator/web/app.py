import os
import json, socket
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import uvicorn


# Where this file lives (inside the installed package)
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# Where migration run reports are written by the CLI/library.
# Global convention: use ./migration_runs (underscore). Users can override with MIGRATION_REPORTS_DIR.

def _get_reports_dir() -> Path:
    env_dir = os.getenv("MIGRATION_REPORTS_DIR")
    if env_dir:
        p = Path(env_dir).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p

    # Canonical default
    underscore = Path("migration_runs").resolve()
    if underscore.exists():
        return underscore

    # Backward-compat: accept legacy hyphenated folder if it exists
    hyphen = Path("migration-runs").resolve()
    if hyphen.exists():
        return hyphen

    # If nothing exists, create the canonical folder
    underscore.mkdir(parents=True, exist_ok=True)
    return underscore

REPORTS_DIR = _get_reports_dir()

app = FastAPI(title="Amplitude Migrator UI", version="1.0")

# Basic permissive CORS so people can run a frontend dev server if they want
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Serve static assets from the installed package
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")

# -------- Helpers --------
def _list_reports():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(REPORTS_DIR.glob("run-*.json"), reverse=True)
    out = []
    for p in files:
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            out.append({
                "id": p.name,
                "started_at": data.get("started_at"),
                "ended_at": data.get("ended_at"),
                "duration_s": data.get("duration_s"),
                "events_read": data.get("counters", {}).get("events_read"),
                "events_kept": data.get("counters", {}).get("events_kept"),
                "events_sent": data.get("counters", {}).get("events_sent"),
                "mtu_estimate": data.get("mtu", {}).get("estimate"),
                "estimated_cost_usd": data.get("mtu", {}).get("estimated_cost_usd"),
                "dry_run": data.get("settings", {}).get("dry_run"),
            })
        except Exception:
            # Ignore malformed files, continue listing
            continue
    return out

# -------- Routes --------
@app.get("/api/migration/runs")
def list_runs():
    return {"runs": _list_reports()}

@app.get("/api/migration/runs/{report_id}")
def get_run(report_id: str):
    path = REPORTS_DIR / report_id
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return JSONResponse(data)

@app.get("/api/migration/run/{name}")
def get_run_by_name(name: str):
    path = REPORTS_DIR / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Report not found")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return JSONResponse(data)

# -------- Entrypoint used by CLI --------
def _find_open_port(host: str, preferred: int, tries: int = 20) -> int:
    """
    Return `preferred` if free, otherwise the next available port within `tries`.
    Raises RuntimeError if none found.
    """
    candidates = [preferred] + list(range(preferred + 1, preferred + tries + 1))
    for p in candidates:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind((host, p))
                return p  # it's free
            except OSError:
                continue
    raise RuntimeError(f"No free port found near {preferred}")

def start_ui(host: str = "127.0.0.1", port: int = 8000, reload: bool = False, auto_port: bool = True):
    """
    Launch the packaged UI. If auto_port=True and `port` is taken, it will try
    subsequent ports (8010, 8011, …) until it finds a free one.
    """
    chosen_port = port
    if auto_port:
        # Prefer a jump to 8010 first to avoid clashing with common 8000 backends
        base = 8010 if port == 8000 else port
        try:
            chosen_port = _find_open_port(host, base, tries=30)
        except RuntimeError:
            # final fallback: just try the originally requested port (may still raise)
            chosen_port = port

    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("Amplitude Migrator UI")
    if host == "0.0.0.0":
        print(f"▶ Local:    http://127.0.0.1:{chosen_port}")
        print(f"▶ Network:  http://{host}:{chosen_port}")
    else:
        print(f"▶ URL:      http://{host}:{chosen_port}")
    print("Reports dir:", REPORTS_DIR)
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    uvicorn.run("amplitude_migrator.web.app:app", host=host, port=chosen_port, reload=reload)