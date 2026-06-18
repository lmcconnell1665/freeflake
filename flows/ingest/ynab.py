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

# The single budget-detail endpoint (/budgets/{id}) returns every entity --
# accounts, payees, transactions, category_groups, categories, and months WITH
# their per-category breakdowns -- in one response, and supports delta loads via
# last_knowledge_of_server. So one call per budget replaces the old per-entity
# calls plus the per-month detail loop, keeping us far under YNAB's 200 req/hour.


# auth
def get_client() -> httpx.Client:
    token = os.getenv("YNAB_TOKEN")
    if not token:
        raise RuntimeError("YNAB_TOKEN is not set")
    return httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=120.0,
    )


# watermarks are stored as {budget_id: server_knowledge} via the shared helpers
# in flows.ingest.watermarks.


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
    return _get(client, "/budgets").json()["data"]["budgets"]


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
def ingest_budget(
    budget_id: str,
    client: httpx.Client,
    last_knowledge: int | None,
    run_ts: str,
) -> int:
    """Pull the full budget in one call and split it into the bronze entities.

    With last_knowledge_of_server set, each array is a delta -- it contains only
    entities changed since the last run (empty arrays are simply skipped, and
    silver keeps the prior snapshot). server_knowledge always comes back and is
    returned as the new watermark.
    """
    params = {}
    if last_knowledge is not None:
        params["last_knowledge_of_server"] = last_knowledge

    data = _get(client, f"/budgets/{budget_id}", params=params).json()["data"]
    budget = data["budget"]

    save_parquet(budget.get("accounts", []), budget_id, "accounts", run_ts)
    save_parquet(budget.get("payees", []), budget_id, "payees", run_ts)
    save_parquet(budget.get("transactions", []), budget_id, "transactions", run_ts)
    save_parquet(budget.get("category_groups", []), budget_id, "category_groups", run_ts)
    save_parquet(budget.get("categories", []), budget_id, "categories", run_ts)

    months = budget.get("months", [])
    # Month summaries without the nested per-category breakdown (that lands in
    # category_months below).
    summaries = [{k: v for k, v in m.items() if k != "categories"} for m in months]
    save_parquet(summaries, budget_id, "months", run_ts)

    # One row per category per (changed) month; carry the month it belongs to.
    category_months = []
    for month in months:
        for category in month.get("categories", []):
            row = dict(category)
            row["_month"] = month["month"]
            category_months.append(row)
    # Entity name "categorymonths" (not "category_months") so its files don't get
    # picked up by the months source glob (*_months_* would otherwise match).
    save_parquet(category_months, budget_id, "categorymonths", run_ts)

    return data["server_knowledge"]


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
        # Old watermarks were a per-entity dict; a non-int means "no usable
        # watermark" → full load, which the budget endpoint does in one call.
        prev = watermarks.get(budget_id)
        last_knowledge = prev if isinstance(prev, int) else None
        watermarks[budget_id] = ingest_budget(budget_id, client, last_knowledge, run_ts)

    save_watermarks(WATERMARK_BLOCK, watermarks)
    client.close()


if __name__ == "__main__":
    ynab_ingest()
