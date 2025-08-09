from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import json
import os

from prefect import flow, task, get_run_logger

# File paths
METRICS_CSV = Path("data/processed/metrics.csv")
STATUS_FILE = Path("data/processed/last_run.json")

# Find the newest CSV file in a folder (not metrics.csv)
@task
def newest_source_csv_in_folder(folder: Path, exclude: Path) -> Path | None:
    logger = get_run_logger()
    candidates = [p for p in folder.glob("*.csv") if p.resolve() != exclude.resolve()]
    if not candidates:
        logger.info("No CSV files found besides metrics.csv.")
        return None
    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    logger.info(f"Newest CSV candidate: {newest.name} (mtime={newest.stat().st_mtime})")
    return newest

# Check if a file is newer than metrics.csv
@task
def is_newer_than_metrics(candidate: Path, metrics_csv: Path) -> bool:
    logger = get_run_logger()
    if not metrics_csv.exists():
        logger.info("metrics.csv not found; treating candidate as new.")
        return True
    cand_mtime = candidate.stat().st_mtime
    metrics_mtime = metrics_csv.stat().st_mtime
    logger.info(f"candidate mtime={cand_mtime}, metrics mtime={metrics_mtime}")
    return cand_mtime > metrics_mtime

# Actually run the cleaner on a file (with retries)
@task(retries=2, retry_delay_seconds=30)
def run_cleaner_on_file(input_csv: Path, output_csv: Path) -> Path:
    logger = get_run_logger()
    logger.info(f"Running run_cleaner on: {input_csv} -> {output_csv}")
    from pipelines.clean_metrics import run_cleaner  # expects your PySpark entrypoint

    # Expecting your function to accept explicit input/output; adjust if your signature differs.
    result = run_cleaner(input_path=str(input_csv), output_path=str(output_csv))
    logger.info(f"run_cleaner result: {result}")
    return Path(result) if isinstance(result, str) else output_csv

# Ensures the output file looks good
@task
def validate_output(path: Path) -> None:
    logger = get_run_logger()
    if not path.exists():
        raise FileNotFoundError(f"Expected output not found: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"Output file is empty: {path}")
    # cheap header peek
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        header = f.readline().strip()
    if not header or "," not in header:

        logger.warning("Output header looks suspicious (no commas found).")
    logger.info(f"Validated output: {path} ({path.stat().st_size} bytes)")

# Write a status file about the last run
@task
def write_status(source_csv: Path, output_csv: Path) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "source_csv": str(source_csv),
        "output_csv": str(output_csv),
        "output_exists": output_csv.exists(),
        "output_size_bytes": output_csv.stat().st_size if output_csv.exists() else 0,
    }
    STATUS_FILE.write_text(json.dumps(payload, indent=2))
    get_run_logger().info(f"Wrote status file: {STATUS_FILE}")

# Main flow to check and clean metrics
@flow(name="check-and-clean-metrics")
def check_and_clean_metrics(metrics_csv: Path = METRICS_CSV) -> Path | None:
    logger = get_run_logger()
    folder = metrics_csv.parent
    folder.mkdir(parents=True, exist_ok=True)

    newest = newest_source_csv_in_folder(folder, metrics_csv)
    if newest is None:
        logger.info("No candidate CSV found. Nothing to do.")
        return None

    if not is_newer_than_metrics(newest, metrics_csv):
        logger.info("No newer CSV than metrics.csv. Nothing to do.")
        return None

    output_path = run_cleaner_on_file(newest, metrics_csv)
    validate_output(output_path)
    write_status(newest, output_path)
    logger.info(f"Done. Updated metrics at: {output_path}")
    return output_path

if __name__ == "__main__":
    # Run locally: python orchestration/check_and_clean.py
    check_and_clean_metrics()
