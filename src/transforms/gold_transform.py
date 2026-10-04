"""
Gold transform — Silver → Gold (data product publish).

Reads the current Silver table, applies final consumer-facing projections,
deduplicates by chapter_id (keeps latest ingest_run_id), and writes:
  lake/gold/university_chapters/v1/

Idempotency strategy: OVERWRITE the v1 partition on each run.
Re-running is safe — no duplicate data accumulates.

Quarantine rows (written to separate path) are NEVER present in Silver,
so they cannot reach Gold.
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("gold_transform")

LAKE_ROOT = Path(__file__).resolve().parents[2] / "lake"

# ---------------------------------------------------------------------------
# Gold schema — the consumer contract
# (mirrors Data Product Contract § Interface)
# ---------------------------------------------------------------------------
GOLD_COLUMNS = [
    "chapter_id",      # STRING  — stable business key (e.g. CA-0355)
    "chapter_name",    # STRING  — display name
    "city",            # STRING  — may be null/UNKNOWN for WARNING rows
    "state",           # STRING  — USPS 2-letter: CA / OR / WA
    "longitude",       # DOUBLE  — WGS84
    "latitude",        # DOUBLE  — WGS84
    "dq_status",       # STRING  — 'OK' | 'WARNING'
    "dq_warnings",     # STRING  — pipe-delimited reason codes ('' for OK)
    "ingest_run_id",   # STRING  — lineage back to bronze run
]

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
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
        SparkSession.builder.appName("university-chapters-gold")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", jvm_opens)
        .config("spark.executor.extraJavaOptions", jvm_opens)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Read Silver
# ---------------------------------------------------------------------------

def load_silver(spark: SparkSession) -> DataFrame:
    silver_path = str(LAKE_ROOT / "silver" / "university_chapters")
    df = spark.read.parquet(silver_path)
    logger.info("Loaded %d rows from Silver", df.count())
    return df


# ---------------------------------------------------------------------------
# Deduplication — keep one row per chapter_id (latest run)
# ---------------------------------------------------------------------------

def deduplicate(df: DataFrame) -> DataFrame:
    """
    If the same chapter_id appeared in multiple ingest runs (e.g. after a
    re-run), keep only the most recent copy (lexicographically largest run_id,
    which is ISO-timestamp-prefixed so this is chronologically latest).
    """
    w = Window.partitionBy("chapter_id").orderBy(F.col("ingest_run_id").desc())
    return (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# CA presence guard
# ---------------------------------------------------------------------------

def assert_ca_present(df: DataFrame) -> None:
    """
    Fail loudly if no CA rows survive to Gold.
    OR/WA being empty is a data fact and is acceptable.
    """
    ca_count = df.filter(F.col("state") == "CA").count()
    if ca_count == 0:
        raise RuntimeError(
            "Gold publish aborted: zero CA rows after DQ filtering. "
            "CA was historically non-zero — investigate upstream data loss."
        )
    logger.info("CA presence check passed — %d CA rows in Gold", ca_count)


# ---------------------------------------------------------------------------
# Write Gold
# ---------------------------------------------------------------------------

def write_gold(df: DataFrame) -> None:
    out = str(LAKE_ROOT / "gold" / "university_chapters" / "v1")
    (
        df.select(GOLD_COLUMNS)
        .write.mode("overwrite")
        .parquet(out)
    )
    logger.info(
        "Gold written → %s (%d rows total)", out, df.count()
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_gold() -> None:
    spark = get_spark()
    silver_df = load_silver(spark)
    gold_df = deduplicate(silver_df)
    assert_ca_present(gold_df)
    write_gold(gold_df)
    spark.stop()


if __name__ == "__main__":
    run_gold()
