import os, json, glob
from fastapi import APIRouter, HTTPException

REPORTS_DIR = os.getenv("MIGRATION_REPORTS_DIR", "migration_runs")
router = APIRouter(prefix="/api/migration", tags=["migration"])

def _list_reports():
    os.makedirs(REPORTS_DIR, exist_ok=True)
    files = sorted(glob.glob(os.path.join(REPORTS_DIR, "run-*.json")), reverse=True)
    return files

@router.get("/runs")
def list_runs():
    out = []
    for path in _list_reports():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            out.append({
                "id": os.path.basename(path),
                "started_at": data.get("started_at"),
                "ended_at": data.get("ended_at"),
                "duration_s": data.get("duration_s"),
                "events_sent": data.get("counters", {}).get("events_sent"),
                "mtu_estimate": data.get("mtu", {}).get("estimate"),
                "estimated_cost_usd": data.get("mtu", {}).get("estimated_cost_usd"),
                "dry_run": data.get("settings", {}).get("dry_run"),
            })
        except Exception:
            continue
    return {"runs": out}

@router.get("/runs/{report_id}")
def get_run(report_id: str):
    path = os.path.join(REPORTS_DIR, report_id)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Report not found")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data