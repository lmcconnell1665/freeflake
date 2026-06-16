from pathlib import Path

from dotenv import load_dotenv
from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

load_dotenv()

# repo_root/dbt
DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"

_settings = PrefectDbtSettings(
    project_dir=str(DBT_DIR),
    profiles_dir=str(DBT_DIR),
)


@flow(name="dbt-build")
def dbt_build(select: str | None = None):
    """Run `dbt build` via PrefectDbtRunner.

    The runner invokes dbt programmatically (not a subprocess), emitting a task run
    and an asset per dbt node, so each model is observable with lineage in the Prefect UI.
    """
    args = ["build"]
    if select:
        args += ["--select", select]
    return PrefectDbtRunner(settings=_settings).invoke(args)


if __name__ == "__main__":
    dbt_build()
