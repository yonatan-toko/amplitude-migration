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
