from datetime import datetime, timezone
from typing import Optional, Dict, Any
import time as _time

def parse_iso_to_ms(value: str | None) -> Optional[int]:
    if not value or not isinstance(value, str):
        return None
    try:
        v = value[:-1] + "+00:00" if value.endswith("Z") else value
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def choose_time_ms(evt: Dict[str, Any], strategy: str) -> int:
    client_ms = evt.get("time") if isinstance(evt.get("time"), int) else None
    srv_recv_ms = parse_iso_to_ms(evt.get("server_received_time"))
    srv_upld_ms = parse_iso_to_ms(evt.get("server_upload_time"))
    s = (strategy or "prefer_client_fallback_server_received").lower()
    now_ms = lambda: int(_time.time() * 1000)

    if s == "client":
        return client_ms if client_ms is not None else now_ms()
    if s == "server_received":
        return srv_recv_ms or client_ms or now_ms()
    if s == "server_upload":
        return srv_upld_ms or client_ms or now_ms()
    if s == "prefer_client_fallback_server_received":
        return client_ms or srv_recv_ms or now_ms()
    if s == "prefer_client_fallback_server_upload":
        return client_ms or srv_upld_ms or now_ms()
    return client_ms or srv_recv_ms or srv_upld_ms or now_ms()
