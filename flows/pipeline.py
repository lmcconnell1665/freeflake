import contextvars
from concurrent.futures import ThreadPoolExecutor, as_completed

from prefect import flow, get_run_logger

from flows.ingest.quickbooks import quickbooks_ingest
from flows.ingest.ynab import ynab_ingest
from flows.ingest.homeassistant import homeassistant_ingest
from flows.ingest.early import early_ingest
from flows.transform.dbt import dbt_build

# Sources are independent: different APIs, bronze dirs, and watermarks, and none
# touch DuckDB (they write Parquet directly). So they run concurrently.
INGESTS = {
    "quickbooks": quickbooks_ingest,
    "ynab": ynab_ingest,
    "homeassistant": homeassistant_ingest,
    "early": early_ingest,
}


@flow(name="freeflake-pipeline")
def freeflake_pipeline():
    """Ingest all sources to bronze concurrently, then transform bronze -> silver/gold.

    Ingests run in parallel; a failure in one is logged and does NOT stop the others
    or the dbt build (silver dedupes, so a failed source just adds no new bronze this
    run). dbt_build is a barrier -- it reads every source's bronze Parquet, so it only
    runs after all ingests have settled.
    """
    logger = get_run_logger()

    with ThreadPoolExecutor(max_workers=len(INGESTS)) as pool:
        # Copy the run context into each worker thread so the ingests nest as
        # subflows of this run rather than detaching into standalone flow runs.
        futures = {
            pool.submit(contextvars.copy_context().run, fn): name
            for name, fn in INGESTS.items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as exc:
                logger.error(f"{name} ingest failed: {exc!r} -- continuing")

    dbt_build()


if __name__ == "__main__":
    freeflake_pipeline()
