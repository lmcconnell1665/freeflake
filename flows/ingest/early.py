import os
from datetime import datetime, timezone
from dotenv import load_dotenv

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from flows.ingest.bronze import bronze_dir, write_parquet

load_dotenv()

RAW_DIR = bronze_dir("early")
BASE_URL = "https://api.early.app/api/v4"
# No modified-since cursor, so each run re-dumps full history (from before the
# product existed) in one ranged call and silver dedupes by id.
EPOCH = datetime(2015, 1, 1, tzinfo=timezone.utc)
API_DT = "%Y-%m-%dT%H:%M:%S.000"


def get_client() -> httpx.Client:
    key, secret = os.getenv("EARLY_API_KEY"), os.getenv("EARLY_API_SECRET")
    if not (key and secret):
        raise RuntimeError("EARLY_API_KEY and EARLY_API_SECRET must be set")
    # key+secret exchange for a short-lived bearer token; mint a fresh one each run.
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


@task(cache_policy=NO_CACHE)
def ingest_activities(client: httpx.Client, run_ts: str):
    resp = client.get("/activities")
    resp.raise_for_status()
    data = resp.json()
    # Archived/inactive activities are still referenced by old time entries — keep all.
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
    write_parquet(RAW_DIR, records, "activities", run_ts)


@task(cache_policy=NO_CACHE)
def ingest_time_entries(client: httpx.Client, run_ts: str):
    now = datetime.now(timezone.utc)
    resp = client.get(f"/time-entries/{EPOCH.strftime(API_DT)}/{now.strftime(API_DT)}")
    resp.raise_for_status()
    write_parquet(RAW_DIR, resp.json()["timeEntries"], "time_entries", run_ts)


@flow(name="early-ingest")
def early_ingest():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_client()
    ingest_activities(client, run_ts)
    ingest_time_entries(client, run_ts)
    client.close()


if __name__ == "__main__":
    early_ingest()
