# Amplitude Migrator — Install & Use Guide

Migrate selected events from one Amplitude project to another with filtering, safe property controls, timestamp strategies, MTU estimates, JSON run reports, and a built‑in web dashboard.  
**You only edit one file: `config.py`. Everything else is created for you.**

---

## 1) Prerequisites

- **Python 3.9+** (`python --version` or `python3 --version`)
- Internet access to Amplitude (connect to VPN if required)
- Project credentials:
  - **Source**: Project **API Key** + **Secret Key** (for Export API)
  - **Destination**: Project **API Key** (for ingestion)
- Regions for each project (**US** or **EU**)

---

## 2) Install (no Git required)

```bash
pip3 install "git+https://github.com/yonatan-toko/amplitude-migration.git@main#egg=amplitude-migrator"
```

This installs the CLI command **`amp-migrate`**.

---

## 3) Create a migration workspace

```bash
amp-migrate init
```

This creates a ready‑to‑use folder in your current directory:

```
amplitude_migration_project/
├── config.py             ← the ONLY file you edit
├── README.md             (auto‑generated; informational)
├── TIME-HANDLING.md      (auto‑generated; informational)
└── migration_runs/       (reports appear here automatically)
```

> You can rename or move this folder anywhere. The tool always reads the paths you give it.

---

## 4) Configure your run

Open and edit **`amplitude_migration_project/config.py`**:

- **Source project**
  - `SOURCE_PROJECT_API_KEY`, `SOURCE_PROJECT_SECRET_KEY`, `SOURCE_REGION` (`"US"` or `"EU"`)
- **Destination project**
  - `DEST_PROJECT_API_KEY`, `DEST_REGION`
- **Choose ONE source input**
  - Use a local export file: set `LOCAL_EXPORT_GZ_PATH = "<path to .json.gz>"`
  - **OR** use the Export API window (hour granularity):
    - `EXPORT_START = "YYYYMMDDTHH"` (e.g., `"20250814T00"`)
    - `EXPORT_END   = "YYYYMMDDTHH"`
- **Event scope (optional)**
  - `EVENT_ALLOWLIST` / `EVENT_DENYLIST`
  - `EVENT_PROPERTY_KEEP` (whitelist only the properties you want to migrate)
  - Optional renames via `EVENT_RENAME_MAP` and `EVENT_PROP_RENAME_MAP`
- **Time handling**
  - `TIME_STRATEGY = "prefer_client_fallback_server_received"` (default)
  - `ORIGINAL_TIMES_AS_PROPERTIES = True` to preserve original timestamps under `event_properties._migration.*`
- **Safety & runtime**
  - Start with `DRY_RUN = True`
  - `REPORTS_DIR = "migration_runs"` (leave as is unless you want a different output folder)

> Tip: You can read secrets from environment variables inside `config.py` if preferred.

---

## 5) Dry run (safe preview)

```bash
cd amplitude_migration_project
amp-migrate --config config.py --dry-run
```

What you’ll see:
- Preview of a few transformed events (no data is sent)
- A JSON report saved to `migration_runs/run-*.json`
- Totals printed, e.g.:
  ```
  Done. read=1234 kept=1200 sent=0 mtu≈812 estimated_cost≈$40.60 (strategy=union, rate=$0.05/MTU)
  ```

---

## 6) Real migration

Set `DRY_RUN = False` in `config.py`, then:

```bash
amp-migrate --config config.py
```

- Events are sent in batches to the destination project
- Final line shows **read/kept/sent**, **MTU estimate**, and **estimated cost**
- Full JSON run report written to `migration_runs/`

---

## 7) Built‑in web dashboard

Start the UI:

```bash
amp-migrate ui
```

Then open **http://127.0.0.1:8000/**

- **Run History**: Lists all JSON reports from `migration_runs/`
- **Details**: Click **View** on any run to see the full summary JSON (counters, MTU estimate, settings snapshot)

> By default the UI reads `migration_runs/` in your **current working directory**.  
> To point the UI at a different folder, set:
> ```bash
> export MIGRATION_REPORTS_DIR=/absolute/path/to/reports
> amp-migrate ui
> ```

---

## 8) Verify in Amplitude

In the **destination project**:
- **Analysis → Events**: confirm migrated event types exist
- **Govern → Data → Properties**: confirm properties (e.g., `doctorName`, `visitDate`)
- Remember: the event’s effective `time` is set by your `TIME_STRATEGY`. If enabled, original timestamps are preserved under `_migration`.

---

## 9) Troubleshooting

- **403 on export**  
  Wrong region or insufficient permissions for Export API. Ensure `SOURCE_REGION` matches your project (US/EU), and you supplied the **Project Secret Key**. If blocked, export via Amplitude UI and use `LOCAL_EXPORT_GZ_PATH`.

- **“Not a gzipped file (b'PK')”**  
  You pointed to a **.zip**. Unzip and use the internal **`.json.gz`** (NDJSON) file.

- **No events migrated**  
  Time window has no data or your allowlist/denylist filtered everything. Test a known hour and loosen filters.

- **UI shows no runs**  
  Confirm you ran a migration and that `migration_runs/` is in the current working directory (or set `MIGRATION_REPORTS_DIR` to the correct path before `amp-migrate ui`).

- **MTU estimate vs billing**  
  MTU is billed monthly per unique active user in the **destination**. The estimate is an upper bound; billing may differ if users already existed that month.

---

## 10) Quick reference — common config keys

- **Source**: `SOURCE_PROJECT_API_KEY`, `SOURCE_PROJECT_SECRET_KEY`, `SOURCE_REGION`
- **Destination**: `DEST_PROJECT_API_KEY`, `DEST_REGION`
- **Input**: `LOCAL_EXPORT_GZ_PATH` **or** (`EXPORT_START`, `EXPORT_END`)
- **Events**: `EVENT_ALLOWLIST`, `EVENT_DENYLIST`, `EVENT_PROPERTY_KEEP`
- **Time**: `TIME_STRATEGY`, `ORIGINAL_TIMES_AS_PROPERTIES`
- **MTU**: `MTU_COUNT_STRATEGY`, `MTU_BILLING_RATE_USD`
- **Runtime**: `DRY_RUN`, `BATCH_SIZE`, `REPORTS_DIR`

---

### TL;DR

1. `pip install amplitude-migrator`  
2. `amp-migrate init` → edit **config.py**  
3. `amp-migrate --config config.py --dry-run`  
4. `amp-migrate --config config.py`  
5. `amp-migrate ui` to review runs in the dashboard

That’s it — one file to edit, everything else handled for you.
