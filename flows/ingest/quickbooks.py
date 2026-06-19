import os
from datetime import datetime, timezone
from dotenv import load_dotenv

from prefect import flow, task
from prefect.variables import Variable

from flows.ingest.bronze import bronze_dir, write_parquet
from flows.ingest.watermarks import load_watermarks, save_watermarks
from intuitlib.client import AuthClient
from quickbooks import QuickBooks
from quickbooks.objects.invoice import Invoice
from quickbooks.objects.payment import Payment
from quickbooks.objects.customer import Customer
from quickbooks.objects.account import Account

load_dotenv()

RAW_DIR = bronze_dir("quickbooks")
WATERMARK_BLOCK = "qb-watermarks"
TOKEN_BLOCK = "qb-tokens"

ENTITIES = {
    "invoices": Invoice,
    "payments": Payment,
    "customers": Customer,
    "accounts": Account,
}


def get_qb_client() -> QuickBooks:
    tokens = load_credentials(TOKEN_BLOCK)
    auth_client = AuthClient(
        client_id=os.getenv("QB_CLIENT_ID"),
        client_secret=os.getenv("QB_CLIENT_SECRET"),
        redirect_uri="https://oauth.pstmn.io/v1/callback",
        environment="production",
        refresh_token=tokens["refresh_token"],
    )
    auth_client.refresh()
    tokens["refresh_token"] = auth_client.refresh_token
    save_credentials(TOKEN_BLOCK, tokens)
    return QuickBooks(auth_client=auth_client, company_id=tokens["company_id"])


def load_credentials(name: str) -> dict:
    value = Variable.get(name, default=None)
    if value is None:
        raise RuntimeError(f"Prefect Variable {name!r} is not set")
    return value


def save_credentials(name: str, value: dict):
    Variable.set(name, value, overwrite=True)


def fetch_since(entity_class, last_updated: str | None, qb: QuickBooks) -> list:
    """Fetch all pages of an entity, filtered by MetaData.LastUpdatedTime when set."""
    where = f"MetaData.LastUpdatedTime > '{last_updated}'" if last_updated else None
    page, results = 1, []
    while True:
        batch = (
            entity_class.where(where, start_position=page, max_results=1000, qb=qb)
            if where
            else entity_class.all(start_position=page, max_results=1000, qb=qb)
        )
        results.extend(batch)
        if len(batch) < 1000:
            break
        page += 1000
    return results


@task
def ingest_entity(
    entity_name: str, entity_class, qb: QuickBooks, watermarks: dict, run_ts: str
) -> str | None:
    last_updated = watermarks.get(entity_name)
    records = fetch_since(entity_class, last_updated, qb)
    write_parquet(RAW_DIR, [r.to_dict() for r in records], entity_name, run_ts)
    # MetaData is an object for some entities, a dict for others — read via to_dict.
    times = [t for r in records if (t := r.to_dict().get("MetaData", {}).get("LastUpdatedTime"))]
    return max(times) if times else last_updated


@flow(name="quickbooks-ingest")
def quickbooks_ingest():
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    qb = get_qb_client()
    watermarks = load_watermarks(WATERMARK_BLOCK)

    new_watermarks = {
        name: ingest_entity(name, cls, qb, watermarks, run_ts)
        for name, cls in ENTITIES.items()
    }

    save_watermarks(WATERMARK_BLOCK, new_watermarks)


if __name__ == "__main__":
    quickbooks_ingest()
