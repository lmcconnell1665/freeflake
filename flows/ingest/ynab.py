import os
import json
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

import httpx
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from flows.ingest.watermarks import load_watermarks, save_watermarks

load_dotenv()

# config
RAW_DIR = Path(os.getenv("DATA_DIR")) / "bronze" / "ynab"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://api.ynab.com/v1"
WATERMARK_BLOCK = "ynab-watermarks"

# entity_name -> (url path segment, response data key)
# all of these support delta requests via last_knowledge_of_server.
ENTITIES = {
    "accounts": ("accounts", "accounts"),
    "categories": ("categories", "category_groups"),
    "payees": ("payees", "payees"),
    "transactions": ("transactions", "transactions"),
    "months": ("months", "months"),
}


# auth
def get_client() -> httpx.Client:
    token = os.getenv("YNAB_TOKEN")
    if not token:
        raise RuntimeError("YNAB_TOKEN is not set")
    return httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60.0,
    )


# watermarks are stored as {budget_id: {entity: server_knowledge}}
# via the shared helpers in flows.ingest.watermarks.


# helpers
def list_budgets(client: httpx.Client) -> list[dict]:
    resp = client.get("/plans")
    resp.raise_for_status()
    return resp.json()["data"]["plans"]


def save_parquet(records: list, budget_id: str, entity: str, run_ts: str):
    if not records:
        return
    df = pd.DataFrame(records)
    nested = [
        c for c in df.columns if df[c].map(lambda v: isinstance(v, (dict, list))).any()
    ]
    for c in nested:
        df[c] = df[c].map(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
    df["_budget_id"] = budget_id
    df["_ingested_at"] = run_ts
    path = RAW_DIR / f"{budget_id}_{entity}_{run_ts}.parquet"
    df.to_parquet(path, index=False)
    print(f"  ✓ {budget_id[:8]} {entity}: {len(df)} records → {path.name}")


# tasks
@task(cache_policy=NO_CACHE)
def ingest_entity(
    budget_id: str,
    entity_name: str,
    client: httpx.Client,
    last_knowledge: int | None,
    run_ts: str,
) -> int | None:
    url_path, data_key = ENTITIES[entity_name]
    params = {}
    if last_knowledge is not None:
        params["last_knowledge_of_server"] = last_knowledge

    resp = client.get(f"/plans/{budget_id}/{url_path}", params=params)
    resp.raise_for_status()
    data = resp.json()["data"]

    save_parquet(data[data_key], budget_id, entity_name, run_ts)
    # advance the watermark; server_knowledge always comes back even on empty deltas.
    return data.get("server_knowledge", last_knowledge)


# flow
@flow(name="ynab-ingest")
def ynab_ingest():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_client()
    watermarks = load_watermarks(WATERMARK_BLOCK)

    budgets = list_budgets(client)
    print(f"Found {len(budgets)} budget(s)")

    for budget in budgets:
        budget_id = budget["id"]
        print(f"Budget: {budget.get('name', budget_id)}")
        save_parquet([budget], budget_id, "budgets", run_ts)
        budget_marks = watermarks.get(budget_id, {})
        new_marks = dict(budget_marks)
        for entity_name in ENTITIES:
            new_marks[entity_name] = ingest_entity(
                budget_id,
                entity_name,
                client,
                budget_marks.get(entity_name),
                run_ts,
            )
        watermarks[budget_id] = new_marks

    save_watermarks(WATERMARK_BLOCK, watermarks)
    client.close()


if __name__ == "__main__":
    ynab_ingest()
