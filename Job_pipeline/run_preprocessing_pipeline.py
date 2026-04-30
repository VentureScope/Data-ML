"""Batch preprocessing runner for all raw CSV files.

Run this file once to process every CSV in Job_pipeline/data/raw and write
feature-only CSV outputs into Job_pipeline/data/processed using the same
filename as each input file.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import List


logger = logging.getLogger(__name__)

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


def process_file(
    input_file: Path,
    output_csv: Path,
    preprocessor: UnifiedPreprocessor,
    max_rows: int | None = None,
) -> tuple[int, int]:
    """Process one file (CSV or JSON) and return number of processed rows."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Processing file: %s -> %s", input_file, output_csv)

    processed_rows = 0
    skipped_rows = 0
    source_name = input_file.stem

    if input_file.suffix.lower() == ".json":
        with input_file.open("r", encoding="utf-8") as f_in:
            data = json.load(f_in)
            if not isinstance(data, list):
                logger.error("JSON file %s must contain a list of objects.", input_file.name)
                return 0, 0
    elif input_file.suffix.lower() == ".csv":
        with input_file.open("r", encoding="utf-8", newline="") as f_in:
            data = list(csv.DictReader(f_in))
    else:
        logger.error("Unsupported file extension: %s", input_file.suffix)
        return 0, 0

    with output_csv.open("w", encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=TARGET_FEATURES)
        writer.writeheader()

        for row in data:
            out = preprocessor.preprocess_row(row, source_name=source_name)
            if out is None:
                skipped_rows += 1
                continue
            writer.writerow({k: _serialize_value(out.get(k)) for k in TARGET_FEATURES})
            processed_rows += 1

            if max_rows is not None and processed_rows >= max_rows:
                logger.info("Row limit reached (%d) for file: %s", max_rows, input_file.name)
                break

    logger.info(
        "Finished file: %s tech_rows=%d skipped_non_tech=%d",
        input_file.name,
        processed_rows,
        skipped_rows,
    )
    return processed_rows, skipped_rows


def list_raw_files(raw_dir: Path) -> List[Path]:
    """Return sorted list of CSV and JSON files in the raw data directory."""
    files = list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.json"))
    return sorted(p for p in files if p.is_file())


def run_batch(
    raw_dir: Path,
    processed_dir: Path,
    max_rows: int | None = None,
    enable_gemini_fallback: bool = False,
) -> None:
    """Run preprocessing for all CSV and JSON files in raw_dir."""
    logger.info(
        "run_batch start raw_dir=%s processed_dir=%s max_rows=%s gemini_fallback=%s",
        raw_dir,
        processed_dir,
        max_rows,
        enable_gemini_fallback,
    )
    preprocessor = UnifiedPreprocessor(
        UnifiedPreprocessorConfig(enable_gemini_fallback=enable_gemini_fallback)
    )
    files = list_raw_files(raw_dir)

    if not files:
        logger.warning("No CSV or JSON files found in: %s", raw_dir)
        print(f"No CSV or JSON files found in: {raw_dir}")
        return

    logger.info("Found %d input files", len(files))
    for input_file in files:
        output_csv = processed_dir / f"{input_file.stem}.csv"
        count, skipped = process_file(
            input_file=input_file,
            output_csv=output_csv,
            preprocessor=preprocessor,
            max_rows=max_rows,
        )
        print(
            f"Processed {count} tech rows (skipped {skipped} non-tech): "
            f"{input_file.name} -> {output_csv.name}"
        )
    logger.info("run_batch complete")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for batch runner."""
    parser = argparse.ArgumentParser(description="Run unified preprocessing on all raw CSV and JSON files.")
    parser.add_argument(
        "--raw-dir",
        default="Job_pipeline/data/raw",
        help="Directory containing input raw CSV and JSON files.",
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
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
