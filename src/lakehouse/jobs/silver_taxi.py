"""Silver layer: typed, deduplicated, quality-gated taxi trips.

Reads from the bronze Iceberg table and writes a silver table with:
  - Consistent column types
  - Rows that pass all quality contracts (invalid rows are dropped and counted)
  - Duplicates removed on the natural trip key

Like the bronze job, every transform here is a pure DataFrame -> DataFrame
function. Only ``run`` touches IO.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from lakehouse.config import JobConfig, load_config
from lakehouse.jobs.ingest_taxi import validate_month
from lakehouse.quality import enforce, no_future_timestamps, not_empty, null_rate, value_range
from lakehouse.session import session_scope
from lakehouse.tables import ensure_namespace, overwrite_partitions

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

TABLE = "yellow_trips_silver"
BRONZE_TABLE = "yellow_trips_bronze"

DEDUP_KEY = ["vendor_id", "pickup_ts", "dropoff_ts", "pickup_location_id", "dropoff_location_id"]


def cast_types(df: DataFrame) -> DataFrame:
    """Coerce numeric columns to their expected types."""
    return (
        df.withColumn("vendor_id", F.col("vendor_id").cast(IntegerType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    )


def apply_quality_rules(df: DataFrame) -> DataFrame:
    """Drop rows that violate trip validity contracts."""
    return df.filter(
        F.col("pickup_ts").isNotNull()
        & F.col("dropoff_ts").isNotNull()
        & (F.col("pickup_ts") < F.col("dropoff_ts"))
        & (F.col("passenger_count") >= 1)
        & (F.col("passenger_count") <= 8)
        & (F.col("trip_distance") > 0)
        & (F.col("total_amount") > 0)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove rows sharing the same natural trip key, keeping one arbitrarily."""
    key = [c for c in DEDUP_KEY if c in df.columns]
    return df.dropDuplicates(key)


def add_silver_columns(df: DataFrame, run_id: str, month: str) -> DataFrame:
    """Attach silver-layer lineage columns."""
    return (
        df.withColumn("_silver_run_id", F.lit(run_id))
        .withColumn("_silver_at", F.current_timestamp())
        .withColumn("_partition_month", F.lit(month))
    )


def transform(df: DataFrame, run_id: str, month: str) -> DataFrame:
    """Full silver transform: cast -> quality filter -> dedup -> lineage."""
    return add_silver_columns(deduplicate(apply_quality_rules(cast_types(df))), run_id, month)


def run(spark: SparkSession, config: JobConfig, month: str, run_id: str | None = None) -> int:
    """Read bronze for *month*, apply silver transforms, and overwrite the silver partition."""
    month = validate_month(month)
    run_id = run_id or str(uuid.uuid4())
    bronze_table = config.table(BRONZE_TABLE)

    log.info("reading bronze %s for month=%s", bronze_table, month)
    bronze = spark.read.table(bronze_table).filter(F.col("_partition_month") == month)

    bronze_count = bronze.count()
    silver = transform(bronze, run_id=run_id, month=month)
    silver_count = silver.count()

    log.info("dropped %d rows (%.1f%%) as invalid or duplicate", bronze_count - silver_count,
             100 * (bronze_count - silver_count) / bronze_count if bronze_count else 0)

    enforce(silver, [
        not_empty(),
        null_rate("vendor_id", max_rate=0.05),
        null_rate("pickup_ts", max_rate=0.0),
        value_range("total_amount", lo=0.01),
        value_range("passenger_count", lo=1, hi=8),
        value_range("trip_distance", lo=0.01),
        no_future_timestamps("pickup_ts"),
    ])

    ensure_namespace(spark, config.catalog.name, config.namespace)
    overwrite_partitions(silver, config.table(TABLE))

    log.info("wrote %d rows to silver (run_id=%s)", silver_count, run_id)
    return silver_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Promote bronze taxi trips to silver")
    parser.add_argument("--config", default="conf/local.yaml")
    parser.add_argument("--month", required=True, help="source month as YYYY-MM")
    parser.add_argument("--master", default=None)
    parser.add_argument("--run-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    with session_scope(config, master=args.master) as spark:
        run(spark, config, month=args.month, run_id=args.run_id)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
