import os
import json
import logging
from pathlib import Path

from dotenv import load_dotenv
from prefect import flow, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

load_dotenv()

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"
MANIFEST = DBT_DIR / "target" / "manifest.json"

# Anchor the scratch DuckDB file to the project dir (profiles.yml's relative path
# otherwise follows cwd). Docker's explicit DBT_DUCKDB_PATH still wins.
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
    """Pre-create each external model's output dir (DuckDB's COPY won't make them).

    Reads the resolved `location` of every external node from the manifest, so it
    stays correct as models are added. Caller must `dbt parse` first.
    """
    log = _logger()
    if not MANIFEST.exists():
        raise FileNotFoundError(f"dbt manifest not found at {MANIFEST}; run `dbt parse` first.")

    manifest = json.loads(MANIFEST.read_text())
    created: list[str] = []
    for node in manifest["nodes"].values():
        if node.get("config", {}).get("materialized") != "external":
            continue
        location = node["config"].get("location")
        parent = Path(location).parent if location else None
        if parent and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
            created.append(str(parent))

    log.info(f"Created {len(created)} external dir(s)" if created else "External dirs present")
    return created


@flow(name="dbt-build")
def dbt_build(select: str | None = None):
    """Run `dbt build` programmatically so each model is an observable Prefect asset."""
    runner = PrefectDbtRunner(settings=_settings)
    runner.invoke(["parse"])
    ensure_external_dirs()

    args = ["build"]
    if select:
        args += ["--select", select]
    return runner.invoke(args)


if __name__ == "__main__":
    dbt_build()
