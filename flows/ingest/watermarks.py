"""Shared helpers for delta-load watermarks stored as Prefect Variables.

Watermarks record how far each ingest has progressed — a LastUpdatedTime per
entity for QuickBooks, a server_knowledge int per budget/entity for YNAB — so
reruns only pull new or changed data. Centralised here so every ingest flow
gets the same first-run and corrupted-state handling, and so that behaviour
is observable in the logs when something goes wrong in production.
"""

from prefect import get_run_logger
from prefect.variables import Variable


def load_watermarks(name: str) -> dict:
    """Load a watermark dict from a Prefect Variable.

    Variable.get returns None (it does NOT raise) when the variable has never
    been set — e.g. the first ever run. We coerce that to an empty dict so the
    flow does a full load, and warn loudly on a non-dict value so a corrupted
    variable surfaces in the run logs instead of blowing up downstream.
    """
    logger = get_run_logger()
    value = Variable.get(name, default=None)
    if value is None:
        logger.info(f"No watermark {name!r} set yet — doing a full load")
        return {}
    if not isinstance(value, dict):
        logger.warning(
            f"Watermark {name!r} is {type(value).__name__}, expected dict — "
            "ignoring it and doing a full load"
        )
        return {}
    logger.info(f"Loaded watermark {name!r} with {len(value)} key(s)")
    return value


def save_watermarks(name: str, value: dict) -> None:
    Variable.set(name, value, overwrite=True)
