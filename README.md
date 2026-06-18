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

## 4. Run scheduled via self-hosted Prefect (Docker)

Prefect runs only in Docker. The same `docker compose up` stack — Postgres + the free,
open-source Prefect server + an in-process worker — runs on your **laptop** (to dev/test)
and on the **sandbox** (for real). The only difference is two values in `.env`. Every run
clones `main` fresh (the `git_clone` step in `prefect.yaml`), so the box always runs
exactly what's pushed — you never edit code on it.

### a. Set the two environment-specific values in `.env`

| var            | laptop                                       | sandbox            |
| -------------- | -------------------------------------------- | ------------------ |
| `DATA_DIR`     | `/Users/you/dev/freeflake/data`              | `/mnt/NeoNAS`      |
| `SSH_KEY_PATH` | path to a deploy key                         | path to a deploy key |

`DATA_DIR` is the **host** folder where Parquet is written; Docker maps it to a fixed
path inside the container, and `make pipeline` writes there directly. `SSH_KEY_PATH` is the
absolute path to a private SSH key whose public half is added as a read-only
[Deploy Key](https://github.com/lmcconnell1665/freeflake/settings/keys) on the repo
(needed for the `git_clone` per-run). On the sandbox, mount the Windows share at
`/mnt/NeoNAS` first (see below).

> DuckDB stays on **local disk** (a Docker volume), never on the share — network file
> locking is unreliable. Only Parquet is written to `HOST_DATA_DIR`.

### b. Bring it up (identical everywhere)

```bash
docker compose up -d --build                                    # start the stack
docker compose exec worker prefect deploy --all                 # register deployment (once)
docker compose logs -f worker                                   # tail runs
```

UI at `http://localhost:4200` (laptop) / `http://<sandbox>:4200`. The worker auto-creates
`freeflake-process-pool`; the deployment carries the daily 6am ET schedule.

### c. Trigger a run

```bash
docker compose exec worker prefect deployment run 'freeflake-pipeline/freeflake-pipeline'
```

This re-clones `main`, so after `git push origin main` a manual trigger (or the 6am
schedule) runs the new code. Reset state with `docker compose down -v`.

### Mounting the NeoNAS share (sandbox only)

```bash
sudo mkdir -p /mnt/NeoNAS
printf 'username=WINUSER\npassword=WINPASS\n' | sudo tee /etc/smb-neonas.creds >/dev/null
sudo chmod 600 /etc/smb-neonas.creds

# /etc/fstab — adjust //SERVER/Share to your actual host + share name:
//NEONAS/DataWarehouse  /mnt/NeoNAS  cifs  credentials=/etc/smb-neonas.creds,uid=1000,gid=1000,iocharset=utf8,vers=3.0,_netdev,nofail  0  0

sudo mount -a
```
