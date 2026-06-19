import os
import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from dotenv import load_dotenv

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from flows.ingest.bronze import bronze_dir, write_parquet
from flows.ingest.watermarks import load_watermarks, save_watermarks

load_dotenv()

RAW_DIR = bronze_dir("homeassistant")
BASE_URL = os.getenv("HA_URL", "http://home.mcc:8123")
WATERMARK_BLOCK = "ha-watermarks"
LABEL = "logged"
# HA's recorder only keeps ~10 days, so reaching further back returns nothing useful.
MAX_LOOKBACK_DAYS = 30


def get_client() -> httpx.Client:
    token = os.getenv("HA_TOKEN")
    if not token:
        raise RuntimeError("HA_TOKEN is not set")
    return httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )


def list_entities(client: httpx.Client, label: str) -> list[str]:
    """Resolve all entity ids carrying `label` via HA's template API."""
    resp = client.post(
        "/api/template",
        json={"template": f"{{{{ label_entities('{label}') | tojson }}}}"},
    )
    resp.raise_for_status()
    return json.loads(resp.text)


@task(cache_policy=NO_CACHE)
def ingest_entity(
    entity_id: str, client: httpx.Client, start: str, end: str, run_ts: str
) -> str:
    """Pull state history in [start, end] for one entity; return new watermark."""
    resp = client.get(
        f"/api/history/period/{quote(start)}",
        params={"filter_entity_id": entity_id, "end_time": end},
    )
    resp.raise_for_status()
    history = resp.json()
    events = history[0] if history else []
    write_parquet(RAW_DIR, events, entity_id.replace(".", "_"), run_ts)
    # Resume from the newest event; if none, keep `start` so idle periods advance.
    times = [e["last_updated"] for e in events if e.get("last_updated")]
    return max(times) if times else start


@flow(name="homeassistant-ingest")
def homeassistant_ingest():
    now = datetime.now(timezone.utc)
    run_ts = now.strftime("%Y%m%dT%H%M%SZ")
    end = now.isoformat()
    floor = (now - timedelta(days=MAX_LOOKBACK_DAYS)).isoformat()

    client = get_client()
    watermarks = load_watermarks(WATERMARK_BLOCK)

    entities = list_entities(client, LABEL)
    print(f"Found {len(entities)} entity(ies) labelled {LABEL!r}")

    new_watermarks = {}
    for entity_id in entities:
        # Resume from the watermark but never past the floor — bounds a catch-up backfill.
        last = watermarks.get(entity_id)
        start = max(last, floor) if last else floor
        new_watermarks[entity_id] = ingest_entity(entity_id, client, start, end, run_ts)

    save_watermarks(WATERMARK_BLOCK, new_watermarks)
    client.close()


if __name__ == "__main__":
    homeassistant_ingest()
