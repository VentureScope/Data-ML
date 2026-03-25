"""Batch preprocessing runner for all raw CSV files.

Run this file once to process every CSV in Job_pipeline/data/raw and write
feature-only CSV outputs into Job_pipeline/data/processed using the same
filename as each input file.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from Job_pipeline.preprocessing.unified_preprocessor import (
    TARGET_FEATURES,
    UnifiedPreprocessor,
    UnifiedPreprocessorConfig,
)


def _serialize_value(value: object) -> str:
    """Serialize output feature values for CSV-safe writing."""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def process_csv_file(
    input_csv: Path,
    output_csv: Path,
    preprocessor: UnifiedPreprocessor,
    max_rows: int | None = None,
) -> int:
    """Process one CSV file and return number of processed rows."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    processed_rows = 0
    source_name = input_csv.stem

    with input_csv.open("r", encoding="utf-8", newline="") as f_in, output_csv.open(
        "w", encoding="utf-8", newline=""
    ) as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=TARGET_FEATURES)
        writer.writeheader()

        for row in reader:
            out = preprocessor.preprocess_row(row, source_name=source_name)
            writer.writerow({k: _serialize_value(out.get(k)) for k in TARGET_FEATURES})
            processed_rows += 1

            if max_rows is not None and processed_rows >= max_rows:
                break

    return processed_rows


def list_raw_csv_files(raw_dir: Path) -> List[Path]:
    """Return sorted list of CSV files in the raw data directory."""
    return sorted(p for p in raw_dir.glob("*.csv") if p.is_file())


def run_batch(
    raw_dir: Path,
    processed_dir: Path,
    max_rows: int | None = None,
    enable_gemini_fallback: bool = False,
) -> None:
    """Run preprocessing for all CSV files in raw_dir."""
    preprocessor = UnifiedPreprocessor(
        UnifiedPreprocessorConfig(enable_gemini_fallback=enable_gemini_fallback)
    )
    files = list_raw_csv_files(raw_dir)

    if not files:
        print(f"No CSV files found in: {raw_dir}")
        return

    for input_csv in files:
        output_csv = processed_dir / input_csv.name
        count = process_csv_file(
            input_csv=input_csv,
            output_csv=output_csv,
            preprocessor=preprocessor,
            max_rows=max_rows,
        )
        print(f"Processed {count} rows: {input_csv.name} -> {output_csv}")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for batch runner."""
    parser = argparse.ArgumentParser(description="Run unified preprocessing on all raw CSV files.")
    parser.add_argument(
        "--raw-dir",
        default="Job_pipeline/data/raw",
        help="Directory containing input raw CSV files.",
    )
    parser.add_argument(
        "--processed-dir",
        default="Job_pipeline/data/processed",
        help="Directory where processed CSV files will be written.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional limit of rows to process per input file.",
    )
    parser.add_argument(
        "--enable-gemini-fallback",
        action="store_true",
        help="Enable Gemini fallback calls during batch processing (slower, network-dependent).",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    processed_dir = Path(args.processed_dir)
    run_batch(
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        max_rows=args.max_rows,
        enable_gemini_fallback=args.enable_gemini_fallback,
    )


if __name__ == "__main__":
    main()
