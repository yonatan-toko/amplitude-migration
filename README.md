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