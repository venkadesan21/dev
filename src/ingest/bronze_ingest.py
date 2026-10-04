"""
Bronze Ingest — university chapters pipeline.

Calls the public ArcGIS FeatureServer REST API for the three west-coast states
(CA, OR, WA), lands the raw JSON payload to the bronze layer, and appends any
synthetic fixture rows so that downstream DQ tests are exercisable offline.

Output path: lake/bronze/university_chapters/<run_id>/data.json
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("bronze_ingest")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services"
    "/UniversityChapters_Public/FeatureServer/0/query"
)
TARGET_STATES = ("CA", "OR", "WA")
# ArcGIS maxRecordCount is 1 000; west-coast volume is well below that.
# Still page defensively via resultOffset so the code is production-ready.
PAGE_SIZE = 1000
REQUEST_TIMEOUT_SECONDS = 30

LAKE_ROOT = Path(__file__).resolve().parents[2] / "lake"
FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "bronze_extras.json"


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _build_query_params(offset: int = 0) -> dict:
    state_list = ", ".join(f"'{s}'" for s in TARGET_STATES)
    return {
        "where": f"State IN ({state_list})",
        "outFields": "*",
        "returnGeometry": "true",
        "resultOffset": offset,
        "resultRecordCount": PAGE_SIZE,
        "f": "json",
    }


def fetch_features() -> list[dict]:
    """Page through the FeatureServer and return all raw feature dicts."""
    all_features: list[dict] = []
    offset = 0

    while True:
        params = _build_query_params(offset)
        logger.info("Fetching features — offset=%d", offset)
        try:
            response = requests.get(
                API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"API request failed: {exc}") from exc

        payload = response.json()

        if "error" in payload:
            raise RuntimeError(f"API returned error: {payload['error']}")

        features = payload.get("features", [])
        all_features.extend(features)
        logger.info("Page returned %d features (total so far: %d)", len(features), len(all_features))

        # Stop when the page is smaller than PAGE_SIZE (last page)
        if len(features) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return all_features


# ---------------------------------------------------------------------------
# Fixture injection (offline / DQ testing)
# ---------------------------------------------------------------------------

def load_fixture_rows() -> list[dict]:
    """Load synthetic rows from fixtures/bronze_extras.json if it exists."""
    if not FIXTURE_PATH.exists():
        logger.info("No fixture file found at %s — skipping injection", FIXTURE_PATH)
        return []
    with FIXTURE_PATH.open() as fh:
        rows = json.load(fh)
    logger.info("Injected %d fixture row(s) from %s", len(rows), FIXTURE_PATH)
    return rows


# ---------------------------------------------------------------------------
# Bronze write
# ---------------------------------------------------------------------------

def write_bronze(features: list[dict], run_id: str, ingest_ts: str) -> Path:
    """
    Persist the raw feature list plus ingest metadata to the bronze layer.

    Returns the output file path.
    """
    out_dir = LAKE_ROOT / "bronze" / "university_chapters" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.json"

    envelope = {
        "ingest_run_id": run_id,
        "ingest_timestamp_utc": ingest_ts,
        "source_url": API_BASE_URL,
        "target_states": list(TARGET_STATES),
        "row_count": len(features),
        "features": features,
    }

    with out_path.open("w") as fh:
        json.dump(envelope, fh, indent=2)

    logger.info("Bronze written → %s (%d rows)", out_path, len(features))
    return out_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_bronze(use_fixtures: bool = True) -> tuple[str, Path]:
    """
    Execute the full bronze ingest.

    Returns (run_id, bronze_path).
    Raises RuntimeError if the API call fails or the combined result set is
    completely empty — an empty publish must be a loud failure, not silent.
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    ingest_ts = datetime.now(timezone.utc).isoformat()

    features = fetch_features()

    if use_fixtures:
        features = features + load_fixture_rows()

    if not features:
        raise RuntimeError(
            "Bronze ingest produced zero rows. API may be down or the filter "
            "returned nothing. Aborting to avoid publishing empty Gold."
        )

    bronze_path = write_bronze(features, run_id, ingest_ts)
    return run_id, bronze_path


if __name__ == "__main__":
    run_id, path = run_bronze()
    print(f"run_id={run_id}  bronze={path}")
