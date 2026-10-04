"""
Silver transform — Bronze → Silver + Quarantine.

Reads the latest (or a specified) bronze run, applies DQ rules, and writes:
  • lake/silver/university_chapters/   — clean + warned rows
  • lake/quarantine/university_chapters/<run_id>/  — hard-failed rows (DQ-Q1)

DQ rules implemented:
  DQ-Q1  (Quarantine) — invalid/missing coordinates
  DQ-W1  (Warning)    — missing or UNKNOWN city

Uses PySpark (local mode) so the transform layer is Spark-shaped and can be
promoted to Databricks / Synapse Spark with zero code changes (change SparkSession
master to the cluster URL or remove it entirely for managed environments).
"""

import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("silver_transform")

LAKE_ROOT = Path(__file__).resolve().parents[2] / "lake"

# ---------------------------------------------------------------------------
# DQ constants
# ---------------------------------------------------------------------------
DQ_REASON_INVALID_COORDINATES = "INVALID_COORDINATES"
DQ_REASON_MISSING_OR_UNKNOWN_CITY = "MISSING_OR_UNKNOWN_CITY"

# ---------------------------------------------------------------------------
# Spark session (local — swap master for managed cluster)
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    # Java 17+ requires explicit module opens for Hadoop UserGroupInformation.
    # These flags are also set via JAVA_TOOL_OPTIONS in CI / local runs.
    jvm_opens = (
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED"
    )
    return (
        SparkSession.builder.appName("university-chapters-silver")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", jvm_opens)
        .config("spark.executor.extraJavaOptions", jvm_opens)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Bronze → flat DataFrame
# ---------------------------------------------------------------------------

def load_bronze(spark: SparkSession, run_id: str) -> DataFrame:
    """
    Read the bronze JSON for *run_id* and flatten to a row-per-feature DataFrame.
    Preserves ingest_run_id for lineage.
    """
    bronze_path = LAKE_ROOT / "bronze" / "university_chapters" / run_id / "data.json"
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

    with bronze_path.open() as fh:
        envelope = json.load(fh)

    features = envelope.get("features", [])
    ingest_run_id = envelope.get("ingest_run_id", run_id)

    # Flatten each ArcGIS feature dict to a plain dict
    rows = []
    for feat in features:
        attrs = feat.get("attributes", {})
        geom = feat.get("geometry") or {}
        rows.append(
            {
                "source_object_id": str(attrs.get("OBJECTID", "")),
                "chapter_id": attrs.get("ChapterID"),
                "chapter_name": attrs.get("University_Chapter"),
                "city": attrs.get("City"),
                "state": attrs.get("State"),
                "raw_longitude": geom.get("x"),
                "raw_latitude": geom.get("y"),
                "ingest_run_id": ingest_run_id,
                # Keep full raw payload for quarantine debugging
                "_raw_feature": json.dumps(feat),
            }
        )

    schema = StructType(
        [
            StructField("source_object_id", StringType(), True),
            StructField("chapter_id", StringType(), True),
            StructField("chapter_name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("raw_longitude", StringType(), True),
            StructField("raw_latitude", StringType(), True),
            StructField("ingest_run_id", StringType(), False),
            StructField("_raw_feature", StringType(), True),
        ]
    )

    df = spark.createDataFrame(rows, schema=schema)
    logger.info("Loaded %d rows from bronze run_id=%s", df.count(), run_id)
    return df


# ---------------------------------------------------------------------------
# DQ-Q1 — coordinate validation (QUARANTINE)
# ---------------------------------------------------------------------------

def apply_dq_q1(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split *df* into (valid, quarantine) based on DQ-Q1.

    Coordinates are invalid when:
      - null / non-numeric
      - longitude ∉ [-180, 180]
      - latitude  ∉ [-90,  90]

    Returns (valid_df, quarantine_df).
    """
    # Cast raw strings to double; non-numeric → null
    df = df.withColumn("longitude", F.col("raw_longitude").cast(DoubleType()))
    df = df.withColumn("latitude", F.col("raw_latitude").cast(DoubleType()))

    coord_ok = (
        F.col("longitude").isNotNull()
        & F.col("latitude").isNotNull()
        & (F.col("longitude") >= -180)
        & (F.col("longitude") <= 180)
        & (F.col("latitude") >= -90)
        & (F.col("latitude") <= 90)
    )

    valid_df = df.filter(coord_ok)
    quarantine_df = (
        df.filter(~coord_ok)
        .withColumn("dq_reason_code", F.lit(DQ_REASON_INVALID_COORDINATES))
        .withColumn("quarantine_ts_utc", F.current_timestamp())
    )
    return valid_df, quarantine_df


# ---------------------------------------------------------------------------
# DQ-W1 — missing/unknown city (WARNING)
# ---------------------------------------------------------------------------

def apply_dq_w1(df: DataFrame) -> DataFrame:
    """
    Tag rows where city is null, blank, or the literal 'UNKNOWN' (case-insensitive).

    Adds/updates:
      dq_status    — 'WARNING' | 'OK'
      dq_warnings  — pipe-separated list of active warning codes (empty string for OK)
    """
    city_bad = (
        F.col("city").isNull()
        | (F.trim(F.col("city")) == "")
        | (F.upper(F.trim(F.col("city"))) == "UNKNOWN")
    )

    df = df.withColumn(
        "dq_status",
        F.when(city_bad, "WARNING").otherwise("OK"),
    )
    df = df.withColumn(
        "dq_warnings",
        F.when(city_bad, DQ_REASON_MISSING_OR_UNKNOWN_CITY).otherwise(""),
    )
    return df


# ---------------------------------------------------------------------------
# Silver write
# ---------------------------------------------------------------------------

SILVER_COLUMNS = [
    "chapter_id",
    "chapter_name",
    "city",
    "state",
    "longitude",
    "latitude",
    "dq_status",
    "dq_warnings",
    "source_object_id",
    "ingest_run_id",
]

QUARANTINE_COLUMNS = [
    "source_object_id",
    "chapter_id",
    "state",
    "raw_longitude",
    "raw_latitude",
    "dq_reason_code",
    "quarantine_ts_utc",
    "ingest_run_id",
    "_raw_feature",
]


def write_silver(df: DataFrame) -> None:
    out = str(LAKE_ROOT / "silver" / "university_chapters")
    (
        df.select(SILVER_COLUMNS)
        .write.mode("overwrite")
        .parquet(out)
    )
    logger.info("Silver written → %s", out)


def write_quarantine(df: DataFrame, run_id: str) -> None:
    if df.rdd.isEmpty():
        logger.info("No quarantine rows for run_id=%s", run_id)
        return
    out = str(LAKE_ROOT / "quarantine" / "university_chapters" / run_id)
    (
        df.select(QUARANTINE_COLUMNS)
        .write.mode("overwrite")
        .parquet(out)
    )
    logger.info("Quarantine written → %s (%d rows)", out, df.count())


# ---------------------------------------------------------------------------
# DQ count logging
# ---------------------------------------------------------------------------

def log_dq_counts(bronze_df: DataFrame, quarantine_df: DataFrame, silver_df: DataFrame) -> dict:
    rows_in = bronze_df.count()
    rows_quarantined = quarantine_df.count()
    rows_warned = silver_df.filter(F.col("dq_status") == "WARNING").count()
    rows_ok = silver_df.filter(F.col("dq_status") == "OK").count()

    counts = {
        "rows_in": rows_in,
        "rows_quarantined": rows_quarantined,
        "rows_warned": rows_warned,
        "rows_ok": rows_ok,
    }
    logger.info(
        "DQ SUMMARY — rows_in=%d rows_quarantined=%d rows_warned=%d rows_ok=%d",
        rows_in,
        rows_quarantined,
        rows_warned,
        rows_ok,
    )

    # Hard fail if ALL rows were quarantined — never silently publish empty Gold
    if rows_quarantined > 0 and (rows_warned + rows_ok) == 0:
        raise RuntimeError(
            "All rows were quarantined. Gold would be empty. Aborting."
        )

    return counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_silver(run_id: str) -> dict:
    """
    Execute the Bronze → Silver transform for the given *run_id*.
    Returns DQ count summary dict.
    """
    spark = get_spark()
    bronze_df = load_bronze(spark, run_id)

    valid_df, quarantine_df = apply_dq_q1(bronze_df)
    silver_df = apply_dq_w1(valid_df)

    counts = log_dq_counts(bronze_df, quarantine_df, silver_df)

    write_silver(silver_df)
    write_quarantine(quarantine_df, run_id)

    spark.stop()
    return counts


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python silver_transform.py <run_id>")
        sys.exit(1)
    result = run_silver(sys.argv[1])
    print(result)
