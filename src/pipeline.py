"""
Pipeline orchestrator — runs Bronze → Silver → Gold in sequence.

Usage:
    python src/pipeline.py              # live API + fixture injection
    python src/pipeline.py --no-fixtures  # live API only
    python src/pipeline.py --bronze-run-id <id>  # reprocess existing bronze run
"""

import argparse
import logging
import sys
from pathlib import Path

# Make src importable when run from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest.bronze_ingest import run_bronze
from transforms.gold_transform import run_gold
from transforms.silver_transform import run_silver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("pipeline")


def main() -> None:
    parser = argparse.ArgumentParser(description="University Chapters Medallion Pipeline")
    parser.add_argument(
        "--no-fixtures",
        action="store_true",
        help="Skip fixture injection (live API data only)",
    )
    parser.add_argument(
        "--bronze-run-id",
        metavar="RUN_ID",
        default=None,
        help="Re-process an existing bronze run instead of calling the API",
    )
    args = parser.parse_args()

    # ── Bronze ──────────────────────────────────────────────────────────────
    if args.bronze_run_id:
        run_id = args.bronze_run_id
        logger.info("Skipping ingest — re-processing existing bronze run_id=%s", run_id)
    else:
        use_fixtures = not args.no_fixtures
        logger.info("Starting Bronze ingest (use_fixtures=%s)", use_fixtures)
        run_id, bronze_path = run_bronze(use_fixtures=use_fixtures)
        logger.info("Bronze complete — run_id=%s path=%s", run_id, bronze_path)

    # ── Silver ───────────────────────────────────────────────────────────────
    logger.info("Starting Silver transform — run_id=%s", run_id)
    dq_counts = run_silver(run_id)
    logger.info("Silver complete — DQ counts: %s", dq_counts)

    # ── Gold ─────────────────────────────────────────────────────────────────
    logger.info("Starting Gold publish")
    run_gold()
    logger.info("Pipeline complete ✓")


if __name__ == "__main__":
    main()
