"""Shared utilities for preprocessing module tests.

Loads a single representative row from the Afriwork raw CSV.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict


RAW_CSV_PATH = Path("Job_pipeline/data/raw/afriwork_tech_jobs_20260310_124718.csv")


def load_sample_row() -> Dict[str, str]:
    """Return the first data row from the Afriwork raw CSV."""
    with RAW_CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        row = next(reader)

    # Add a stable source value for downstream modules that rely on source.
    row = dict(row)
    row.setdefault("source", "afriwork")
    row["source"] = row.get("source") or "afriwork"
    return row
