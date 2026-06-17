import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
from dotenv import load_dotenv

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from flows.ingest.watermarks import load_watermarks, save_watermarks

load_dotenv()

# config
RAW_DIR = Path(os.getenv("DATA_DIR")) / "bronze" / "homeassistant"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = os.getenv("HA_URL", "http://home.mcc:8123")
WATERMARK_BLOCK = "ha-watermarks"
LABEL = "logged"
# Cap first-run / long-gap backfills. HA's recorder only keeps ~10 days by
# default, so reaching further back just returns nothing useful anyway.
MAX_LOOKBACK_DAYS = 30


# auth
def get_client() -> httpx.Client:
    token = os.getenv("HA_TOKEN")
    if not token:
        raise RuntimeError("HA_TOKEN is not set")
    return httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )


# watermarks are stored as {entity_id: last_updated_iso} via the shared
# helpers in flows.ingest.watermarks — each entity tracked independently so a
# noisy sensor never holds back a quiet one.


# helpers
def list_entities(client: httpx.Client, label: str) -> list[str]:
    """Resolve all entity ids carrying `label` via HA's template API."""
    resp = client.post(
        "/api/template",
        json={"template": f"{{{{ label_entities('{label}') | tojson }}}}"},
    )
    resp.raise_for_status()
    return json.loads(resp.text)


def save_parquet(records: list, entity_id: str, run_ts: str):
    if not records:
        return
    df = pd.DataFrame(records)
    nested = [
        c for c in df.columns if df[c].map(lambda v: isinstance(v, (dict, list))).any()
    ]
    for c in nested:
        df[c] = df[c].map(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
    df["_ingested_at"] = run_ts
    path = RAW_DIR / f"{entity_id.replace('.', '_')}_{run_ts}.parquet"
    df.to_parquet(path, index=False)
    print(f"  ✓ {entity_id}: {len(df)} records → {path.name}")


# tasks
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
    # History comes back as one list per requested entity; we request just one.
    events = history[0] if history else []

    save_parquet(events, entity_id, run_ts)
    # Advance to the newest event seen so the next run resumes from there;
    # if nothing changed, keep `start` so a long idle period still moves on.
    times = [e["last_updated"] for e in events if e.get("last_updated")]
    return max(times) if times else start


# flow
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
        # Resume from the watermark, but never reach past the lookback floor —
        # this is what lets a missed run catch up without an unbounded backfill.
        last = watermarks.get(entity_id)
        start = max(last, floor) if last else floor
        new_watermarks[entity_id] = ingest_entity(entity_id, client, start, end, run_ts)

    save_watermarks(WATERMARK_BLOCK, new_watermarks)
    client.close()


if __name__ == "__main__":
    homeassistant_ingest()
