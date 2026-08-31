"""Gold layer: zone-level revenue aggregates and trip duration percentiles.

Reads from the silver Iceberg table and writes two gold tables:

  yellow_trips_gold_revenue   -- total and average revenue per pickup zone per month
  yellow_trips_gold_durations -- p50/p75/p95 trip duration in minutes per zone per month

Both tables are overwritten each run, so the job is idempotent. Downstream BI
tools query gold directly; they never touch silver or bronze.

Transforms are pure DataFrame -> DataFrame functions, consistent with the rest
of the pipeline.
"""

from __future__ import annotations

import argparse
import logging
import uuid
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from lakehouse.config import JobConfig, load_config
from lakehouse.jobs.ingest_taxi import validate_month
from lakehouse.session import session_scope
from lakehouse.tables import ensure_namespace, overwrite_partitions

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

REVENUE_TABLE = "yellow_trips_gold_revenue"
DURATION_TABLE = "yellow_trips_gold_durations"
SILVER_TABLE = "yellow_trips_silver"


def zone_revenue(df: DataFrame) -> DataFrame:
    """Aggregate total and average revenue by pickup zone and partition month."""
    return df.groupBy("pickup_location_id", "_partition_month").agg(
        F.count("*").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_revenue"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
    )


def zone_duration_percentiles(df: DataFrame) -> DataFrame:
    """Compute p50/p75/p95 trip duration in minutes by pickup zone and month."""
    with_duration = df.withColumn(
        "duration_minutes",
        F.round(
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60,
            1,
        ),
    )
    return with_duration.groupBy("pickup_location_id", "_partition_month").agg(
        F.count("*").alias("trip_count"),
        F.round(F.percentile_approx("duration_minutes", 0.50), 1).alias("p50_minutes"),
        F.round(F.percentile_approx("duration_minutes", 0.75), 1).alias("p75_minutes"),
        F.round(F.percentile_approx("duration_minutes", 0.95), 1).alias("p95_minutes"),
    )


def run(spark: SparkSession, config: JobConfig, month: str, run_id: str | None = None) -> dict:
    """Read silver for *month* and write both gold aggregates."""
    month = validate_month(month)
    run_id = run_id or str(uuid.uuid4())
    silver_table = config.table(SILVER_TABLE)

    log.info("reading silver %s for month=%s", silver_table, month)
    silver = spark.read.table(silver_table).filter(F.col("_partition_month") == month)

    ensure_namespace(spark, config.catalog.name, config.namespace)

    revenue = zone_revenue(silver)
    overwrite_partitions(revenue, config.table(REVENUE_TABLE))
    revenue_rows = revenue.count()
    log.info("wrote %d zone-revenue rows (run_id=%s)", revenue_rows, run_id)

    durations = zone_duration_percentiles(silver)
    overwrite_partitions(durations, config.table(DURATION_TABLE))
    duration_rows = durations.count()
    log.info("wrote %d zone-duration rows (run_id=%s)", duration_rows, run_id)

    return {"revenue_rows": revenue_rows, "duration_rows": duration_rows}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build gold aggregates from silver taxi trips")
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
