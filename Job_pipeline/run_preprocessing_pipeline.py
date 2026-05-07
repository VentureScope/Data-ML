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
from typing import List, Set


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


def load_progress(progress_file: Path) -> dict:
    """Load the progress dictionary from file."""
    if progress_file.exists():
        try:
            with open(progress_file, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning("Failed to decode progress.json, starting fresh.")
            pass
    return {}


def save_progress(progress_file: Path, progress: dict) -> None:
    """Save the progress dictionary safely."""
    tmp_file = progress_file.with_suffix(".tmp")
    with open(tmp_file, "w") as f:
        json.dump(progress, f, indent=2)
    tmp_file.replace(progress_file)


def process_file(
    input_file: Path,
    output_csv: Path,
    preprocessor: UnifiedPreprocessor,
    seen_job_ids: Set[str],
    progress: dict,
    progress_file: Path,
    max_rows: int | None = None,
) -> tuple[int, int]:
    """Process one file (CSV or JSON) and return number of processed rows."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    start_idx = progress.get(input_file.name, 0)
    if start_idx > 0:
        logger.info("Resuming file: %s from index %d -> %s", input_file, start_idx, output_csv)
    else:
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

    mode = "a" if start_idx > 0 and output_csv.exists() else "w"

    with output_csv.open(mode, encoding="utf-8", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=TARGET_FEATURES)
        if mode == "w":
            writer.writeheader()

        for i, row in enumerate(data[start_idx:]):
            actual_idx = start_idx + i
            out = preprocessor.preprocess_row(row, source_name=source_name)
            
            if out is not None:
                job_id = str(out.get("job_id", ""))
                if job_id not in seen_job_ids:
                    seen_job_ids.add(job_id)
                    writer.writerow({k: _serialize_value(out.get(k)) for k in TARGET_FEATURES})
                    processed_rows += 1
                else:
                    skipped_rows += 1
            else:
                skipped_rows += 1
                
            # Update progress
            progress[input_file.name] = actual_idx + 1
            if (actual_idx + 1) % 50 == 0:
                f_out.flush()
                save_progress(progress_file, progress)

            if max_rows is not None and processed_rows >= max_rows:
                logger.info("Row limit reached (%d) for file: %s", max_rows, input_file.name)
                break

    # Save final progress for this file
    save_progress(progress_file, progress)

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
    enable_llm_fallback: bool = False,
) -> None:
    """Run preprocessing for all CSV and JSON files in raw_dir."""
    logger.info(
        "run_batch start raw_dir=%s processed_dir=%s max_rows=%s llm_fallback=%s",
        raw_dir,
        processed_dir,
        max_rows,
        enable_llm_fallback,
    )
    preprocessor = UnifiedPreprocessor(
        UnifiedPreprocessorConfig(enable_llm_fallback=enable_llm_fallback)
    )
    files = list_raw_files(raw_dir)

    if not files:
        logger.warning("No CSV or JSON files found in: %s", raw_dir)
        print(f"No CSV or JSON files found in: {raw_dir}")
        return

    logger.info("Found %d input files", len(files))
    
    seen_job_ids: Set[str] = set()
    for existing_csv in processed_dir.glob("*.csv"):
        try:
            with open(existing_csv, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("job_id"):
                        seen_job_ids.add(row["job_id"])
        except Exception as e:
            logger.warning("Could not read %s to rebuild seen_job_ids: %s", existing_csv.name, e)
    
    logger.info("Restored %d seen job_ids from existing processed files.", len(seen_job_ids))

    progress_file = processed_dir / "progress.json"
    progress = load_progress(progress_file)

    for input_file in files:
        output_csv = processed_dir / f"{input_file.stem}_processed.csv"
        count, skipped = process_file(
            input_file=input_file,
            output_csv=output_csv,
            preprocessor=preprocessor,
            seen_job_ids=seen_job_ids,
            progress=progress,
            progress_file=progress_file,
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
        "--enable-llm-fallback",
        action="store_true",
        help="Enable Groq LLM fallback calls during batch processing (slower, network-dependent).",
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
        enable_llm_fallback=args.enable_llm_fallback,
    )


if __name__ == "__main__":
    main()
