import os
import json
import time
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

import httpx
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE

from flows.ingest.watermarks import load_watermarks, save_watermarks

load_dotenv()

# config
RAW_DIR = Path(os.getenv("DATA_DIR")) / "bronze" / "ynab"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://api.ynab.com/v1"
WATERMARK_BLOCK = "ynab-watermarks"

# YNAB allows 200 requests/hour per token. The per-month detail endpoint (used
# for category_months) has no delta support, so after a one-time full backfill we
# only re-fetch the most recent N months each run -- closed months rarely change
# and are already captured in earlier snapshots (silver dedupes to the latest).
MONTHS_LOOKBACK = int(os.getenv("YNAB_MONTHS_LOOKBACK", "3"))
# Marker stored in the per-budget watermarks once every month has been fetched.
BACKFILL_MARK = "category_months_backfilled"

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
def _get(client: httpx.Client, url: str, params: dict | None = None, max_retries: int = 5):
    """GET with backoff on 429. Honors a `Retry-After` header when present,
    otherwise backs off exponentially (capped). Raises for any other 4xx/5xx."""
    for attempt in range(max_retries + 1):
        resp = client.get(url, params=params)
        if resp.status_code == 429 and attempt < max_retries:
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(2**attempt, 60)
            try:
                get_run_logger().warning(
                    f"YNAB 429 on {url}; retrying in {delay:.0f}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
            except Exception:
                pass
            time.sleep(delay)
            continue
        resp.raise_for_status()
        return resp
    # Exhausted retries on 429: surface it.
    resp.raise_for_status()
    return resp


def list_budgets(client: httpx.Client) -> list[dict]:
    return _get(client, "/plans").json()["data"]["plans"]


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

    data = _get(client, f"/plans/{budget_id}/{url_path}", params=params).json()["data"]

    save_parquet(data[data_key], budget_id, entity_name, run_ts)
    # advance the watermark; server_knowledge always comes back even on empty deltas.
    return data.get("server_knowledge", last_knowledge)


@task(cache_policy=NO_CACHE)
def ingest_category_months(
    budget_id: str,
    client: httpx.Client,
    run_ts: str,
    full_backfill: bool,
) -> None:
    # Per-category, per-month balances are only exposed by YNAB's single-month
    # detail endpoint -- the months *list* returns summaries with no category
    # breakdown. There's no last_knowledge_of_server delta support here, so each
    # month costs one request. To stay under YNAB's 200 req/hour limit we fetch
    # every month only on the first run (full_backfill); afterwards just the most
    # recent MONTHS_LOOKBACK. Older closed months are already captured in earlier
    # snapshots and silver dedupes to the latest.
    months = _get(client, f"/plans/{budget_id}/months").json()["data"]["months"]

    active = sorted(
        (m for m in months if not m.get("deleted")), key=lambda m: m["month"]
    )
    if not full_backfill:
        active = active[-MONTHS_LOOKBACK:]

    records = []
    for month in active:
        month_date = month["month"]
        detail = _get(client, f"/plans/{budget_id}/months/{month_date}")
        for category in detail.json()["data"]["month"]["categories"]:
            row = dict(category)
            row["_month"] = month_date
            records.append(row)

    save_parquet(records, budget_id, "category_months", run_ts)


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
        # category-month balances have no delta support: full fetch once, then
        # only the recent window on later runs.
        full_backfill = not budget_marks.get(BACKFILL_MARK, False)
        ingest_category_months(budget_id, client, run_ts, full_backfill)
        new_marks[BACKFILL_MARK] = True
        watermarks[budget_id] = new_marks

    save_watermarks(WATERMARK_BLOCK, watermarks)
    client.close()


if __name__ == "__main__":
    ynab_ingest()
