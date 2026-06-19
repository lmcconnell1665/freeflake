"""Delta-load watermarks stored as Prefect Variables, shared across ingest flows.

A watermark records how far an ingest has progressed so reruns only pull new data.
Centralised for consistent first-run/corrupted-state handling, logged for prod visibility.
"""

from prefect import get_run_logger
from prefect.variables import Variable


def load_watermarks(name: str) -> dict:
    """Return the watermark dict, or {} (full load) when unset or corrupted."""
    logger = get_run_logger()
    value = Variable.get(name, default=None)
    if value is None:
        logger.info(f"No watermark {name!r} set yet — doing a full load")
        return {}
    if not isinstance(value, dict):
        logger.warning(f"Watermark {name!r} is {type(value).__name__}, expected dict — full load")
        return {}
    logger.info(f"Loaded watermark {name!r} with {len(value)} key(s)")
    return value


def save_watermarks(name: str, value: dict) -> None:
    Variable.set(name, value, overwrite=True)
