"""Shared bronze-layer helpers: resolve a source's landing dir and write Parquet."""

import os
import json
from pathlib import Path

import pandas as pd


def bronze_dir(source: str) -> Path:
    path = Path(os.environ["DATA_DIR"]) / "bronze" / source
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_parquet(raw_dir: Path, records: list, name: str, run_ts: str, **extra) -> None:
    """Write `records` to {name}_{run_ts}.parquet, JSON-encoding nested columns."""
    if not records:
        return
    df = pd.DataFrame(records)
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, (dict, list))).any():
            df[col] = df[col].map(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
    for key, value in extra.items():
        df[key] = value
    df["_ingested_at"] = run_ts
    path = raw_dir / f"{name}_{run_ts}.parquet"
    df.to_parquet(path, index=False)
    print(f"  ✓ {name}: {len(df)} → {path.name}")
