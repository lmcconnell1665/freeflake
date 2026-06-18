import os
import json
import logging
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

load_dotenv()

# repo_root/dbt
DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
MANIFEST = DBT_DIR / "target" / "manifest.json"

# Anchor the (throwaway) DuckDB scratch file to the dbt project dir. profiles.yml's
# default path is relative, so it would otherwise follow the process cwd and land
# wherever the flow happens to run from (e.g. the repo root). An explicit
# DBT_DUCKDB_PATH (Docker sets one) still wins.
os.environ.setdefault("DBT_DUCKDB_PATH", str(DBT_DIR / "warehouse.duckdb"))

_settings = PrefectDbtSettings(
    project_dir=str(DBT_DIR),
    profiles_dir=str(DBT_DIR),
)


def _logger():
    """Prefect run logger when inside a flow, plain logger otherwise (e.g. Makefile)."""
    try:
        return get_run_logger()
    except Exception:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("dbt-build")


def ensure_external_dirs() -> list[str]:
    """Pre-create the parent directory of every external model's output.

    DuckDB's `COPY ... TO 'file.parquet'` does not create missing parent
    directories, so the first run of a new silver source or gold mart fails with
    an IO error until someone makes the folder by hand. We read the resolved
    `location` of each external model from the dbt manifest (the source of truth,
    so this stays correct as models are added) and mkdir its parent. Idempotent.

    Callers must refresh the manifest (`dbt parse`) first so newly added models
    are included.
    """
    log = _logger()
    if not MANIFEST.exists():
        raise FileNotFoundError(
            f"dbt manifest not found at {MANIFEST}; run `dbt parse` before ensuring dirs."
        )

    manifest = json.loads(MANIFEST.read_text())
    created: list[str] = []
    seen: set[Path] = set()
    for node in manifest["nodes"].values():
        config = node.get("config", {})
        if config.get("materialized") != "external":
            continue
        location = config.get("location")
        if not location:
            continue
        parent = Path(location).parent
        if parent in seen:
            continue
        seen.add(parent)
        if not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
            created.append(str(parent))

    if created:
        log.info(f"Created {len(created)} external dir(s): {', '.join(sorted(created))}")
    else:
        log.info("External output dirs already present")
    return created


@flow(name="dbt-build")
def dbt_build(select: str | None = None):
    """Run `dbt build` via PrefectDbtRunner.

    The runner invokes dbt programmatically (not a subprocess), emitting a task run
    and an asset per dbt node, so each model is observable with lineage in the Prefect UI.
    """
    runner = PrefectDbtRunner(settings=_settings)
    # Refresh the manifest, then make sure every external output dir exists before
    # any model tries to COPY into it.
    runner.invoke(["parse"])
    ensure_external_dirs()

    args = ["build"]
    if select:
        args += ["--select", select]
    return runner.invoke(args)


if __name__ == "__main__":
    dbt_build()
