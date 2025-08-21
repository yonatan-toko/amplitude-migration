import os
import json
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import uvicorn

# Where this file lives (inside the installed package)
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

# Where migration run reports are written by the CLI/library
# (defaults to ./migration_runs relative to the current working dir)
REPORTS_DIR = Path(os.getenv("MIGRATION_REPORTS_DIR", "migration_runs")).resolve()

app = FastAPI(title="Amplitude Migrator UI", version="1.0")

# Basic permissive CORS so people can run a frontend dev server if they want
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Serve static assets from the installed package
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

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
@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")

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

# -------- Entrypoint used by CLI --------
def start_ui(host: str = "127.0.0.1", port: int = 8000, reload: bool = False):
    """
    Launch the packaged UI. Called by `amp-migrate ui`.
    """
    uvicorn.run("amplitude_migrator.web.app:app", host=host, port=port, reload=reload)