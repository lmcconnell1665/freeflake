from prefect import flow

from flows.ingest.quickbooks import quickbooks_ingest
from flows.ingest.ynab import ynab_ingest
from flows.transform.dbt import dbt_build


@flow(name="freeflake-pipeline")
def freeflake_pipeline():
    """Ingest all sources to bronze, then transform bronze -> silver/gold with dbt."""
    quickbooks_ingest()
    ynab_ingest()
    dbt_build()


if __name__ == "__main__":
    freeflake_pipeline()
