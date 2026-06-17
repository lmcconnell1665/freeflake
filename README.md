# freeflake

Data warehousing solution built on DuckDB + dbt, orchestrated with Prefect, to run my
consulting agency analytics for ~nearly~ free.

Sources are ingested to a **bronze** layer in a local DuckDB file, then dbt transforms
them into **silver** and **gold** Parquet tables written to `DATA_DIR` (a local folder in
dev, an SMB file share in prod).

## Prerequisites

- Python 3.12
- Git

## 1. Clone and install

```bash
git clone git@github.com:lmcconnell1665/freeflake.git
cd freeflake
make setup     # creates .venv
source .venv/bin/activate
make install   # installs requirements.txt
```

## 2. Configure environment

Copy the example file and fill in your own values (API tokens, and `DATA_DIR` — where the
silver/gold Parquet files get written):

```bash
cp .env.example .env
# then edit .env
```

## 3. Run the pipeline locally (quickest end-to-end test)

This runs the full flow once in your terminal — all ingests, then `dbt build`:

```bash
make pipeline
```

Output Parquet lands under `DATA_DIR/silver/...` and `DATA_DIR/gold/...`; the DuckDB file
(`warehouse.duckdb`) stays local.

## 4. Run it on a schedule via self-hosted Prefect

This uses the free, open-source Prefect server (no Prefect Cloud). The server holds the
schedule (daily at 6am ET) and queues runs onto the `freeflake-process-pool` work pool. A
local worker picks up each run, clones the repo fresh, and executes the flow.

**a. Start the Prefect server** (leave running in its own terminal; serves the UI at
http://127.0.0.1:4200):

```bash
prefect server start
```

**b. Point the CLI/worker at the local server** (in your other terminals):

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

**c. Create the deployment** (reads `prefect.yaml`):

```bash
make deploy
```

**d. Start a worker** to pull and execute runs (leave this running; on the prod Linux
server this is a systemd service). It needs the `.env` values in its environment:

```bash
set -a && . ./.env && set +a
prefect worker start --pool freeflake-process-pool
```

**e. Trigger a run now** (or just wait for the 6am schedule):

```bash
prefect deployment run 'freeflake-pipeline/freeflake-pipeline'
```

Watch progress, logs, and run history in the Prefect UI at http://127.0.0.1:4200.
