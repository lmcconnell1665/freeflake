import os
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

load_dotenv()

# config
RAW_DIR = Path(os.getenv("DATA_DIR")) / "bronze" / "early"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://api.early.app/api/v4"
# Early's API can only filter time entries by when they occurred — there is no
# "modified since" cursor — so each run re-dumps the full history and silver
# dedupes by id. We sweep from before the product existed (Timeular launched
# ~2015) to now, so every entry is always captured; CHUNK_DAYS keeps each
# response small.
EPOCH = datetime(2015, 1, 1, tzinfo=timezone.utc)
CHUNK_DAYS = 90
API_DT = "%Y-%m-%dT%H:%M:%S.000"  # Early wants millisecond ISO, no timezone


# auth
def get_client() -> httpx.Client:
    key, secret = os.getenv("EARLY_API_KEY"), os.getenv("EARLY_API_SECRET")
    if not (key and secret):
        raise RuntimeError("EARLY_API_KEY and EARLY_API_SECRET must be set")
    # Sign-in is stateless: key+secret exchange for a short-lived bearer token,
    # so we just mint a fresh one each run rather than caching it.
    resp = httpx.post(
        f"{BASE_URL}/developer/sign-in",
        json={"apiKey": key, "apiSecret": secret},
        timeout=60.0,
    )
    resp.raise_for_status()
    token = resp.json()["token"]
    return httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )


# helpers
def save_parquet(records: list, entity: str, run_ts: str):
    if not records:
        return
    df = pd.DataFrame(records)
    nested = [
        c for c in df.columns if df[c].map(lambda v: isinstance(v, (dict, list))).any()
    ]
    for c in nested:
        df[c] = df[c].map(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
    df["_ingested_at"] = run_ts
    path = RAW_DIR / f"{entity}_{run_ts}.parquet"
    df.to_parquet(path, index=False)
    print(f"  ✓ {entity}: {len(df)} records → {path.name}")


# tasks
@task(cache_policy=NO_CACHE)
def ingest_activities(client: httpx.Client, run_ts: str):
    resp = client.get("/activities")
    resp.raise_for_status()
    data = resp.json()
    # /activities splits results across three buckets; archived/inactive ones are
    # still referenced by historical time entries, so keep them all and tag status.
    statuses = {
        "activities": "active",
        "inactiveActivities": "inactive",
        "archivedActivities": "archived",
    }
    records = [
        {**a, "status": status}
        for key, status in statuses.items()
        for a in data.get(key, [])
    ]
    save_parquet(records, "activities", run_ts)


@task(cache_policy=NO_CACHE)
def ingest_time_entries(client: httpx.Client, run_ts: str):
    start = EPOCH
    now = datetime.now(timezone.utc)
    entries = []
    while start < now:
        end = min(start + timedelta(days=CHUNK_DAYS), now)
        resp = client.get(f"/time-entries/{start.strftime(API_DT)}/{end.strftime(API_DT)}")
        resp.raise_for_status()
        entries.extend(resp.json()["timeEntries"])
        start = end
    # Entries overlapping a chunk boundary come back in both windows — last wins.
    entries = list({e["id"]: e for e in entries}.values())
    save_parquet(entries, "time_entries", run_ts)


# flow
@flow(name="early-ingest")
def early_ingest():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_client()
    ingest_activities(client, run_ts)
    ingest_time_entries(client, run_ts)
    client.close()


if __name__ == "__main__":
    early_ingest()
