import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

import httpx
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE

from flows.ingest.bronze import bronze_dir, write_parquet
from flows.ingest.watermarks import load_watermarks, save_watermarks

load_dotenv()

RAW_DIR = bronze_dir("ynab")
BASE_URL = "https://api.ynab.com/v1"
WATERMARK_BLOCK = "ynab-watermarks"
# The /budgets/{id} detail endpoint returns every entity (incl. months with their
# per-category breakdown) in one delta-aware call, replacing the old per-entity
# calls plus per-month loop and keeping us well under YNAB's 200 req/hour.


def get_client() -> httpx.Client:
    token = os.getenv("YNAB_TOKEN")
    if not token:
        raise RuntimeError("YNAB_TOKEN is not set")
    return httpx.Client(
        base_url=BASE_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=120.0,
    )


def _get(client: httpx.Client, url: str, params: dict | None = None, max_retries: int = 5):
    """GET with backoff on 429 (honoring Retry-After); raises on any other error."""
    for attempt in range(max_retries + 1):
        resp = client.get(url, params=params)
        if resp.status_code == 429 and attempt < max_retries:
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(2**attempt, 60)
            try:
                get_run_logger().warning(f"YNAB 429 on {url}; retrying in {delay:.0f}s")
            except Exception:
                pass
            time.sleep(delay)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


def list_budgets(client: httpx.Client) -> list[dict]:
    return _get(client, "/budgets").json()["data"]["budgets"]


@task(cache_policy=NO_CACHE)
def ingest_budget(
    budget_id: str, client: httpx.Client, last_knowledge: int | None, run_ts: str
) -> int:
    """Pull the full budget in one call and split it into bronze entities.

    With last_knowledge_of_server set, each array is a delta (changed entities
    only); empty arrays are skipped and silver keeps the prior snapshot.
    """
    params = {"last_knowledge_of_server": last_knowledge} if last_knowledge is not None else {}
    data = _get(client, f"/budgets/{budget_id}", params=params).json()["data"]
    budget = data["budget"]

    def write(records, entity):
        write_parquet(RAW_DIR, records, f"{budget_id}_{entity}", run_ts, _budget_id=budget_id)

    for entity in ("accounts", "payees", "transactions", "category_groups", "categories"):
        write(budget.get(entity, []), entity)

    months = budget.get("months", [])
    write([{k: v for k, v in m.items() if k != "categories"} for m in months], "months")

    # Entity name "categorymonths" avoids the months source glob (*_months_*).
    category_months = [
        {**category, "_month": month["month"]}
        for month in months
        for category in month.get("categories", [])
    ]
    write(category_months, "categorymonths")

    return data["server_knowledge"]


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
        write_parquet(RAW_DIR, [budget], f"{budget_id}_budgets", run_ts, _budget_id=budget_id)
        # A non-int (old per-entity dict / unset) means full load.
        prev = watermarks.get(budget_id)
        last_knowledge = prev if isinstance(prev, int) else None
        watermarks[budget_id] = ingest_budget(budget_id, client, last_knowledge, run_ts)

    save_watermarks(WATERMARK_BLOCK, watermarks)
    client.close()


if __name__ == "__main__":
    ynab_ingest()
