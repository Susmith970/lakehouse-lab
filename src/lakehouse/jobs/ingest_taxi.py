"""Bronze ingestion: NYC TLC yellow taxi trips -> Iceberg.

Design note: every transform here is a pure DataFrame -> DataFrame function so
the logic is unit-testable against a plain local SparkSession, with no Iceberg
jars or network access required. Only ``run`` touches IO.
"""

from __future__ import annotations

import argparse
import logging
import re
import uuid
from typing import TYPE_CHECKING

from lakehouse.config import JobConfig, load_config
from lakehouse.session import session_scope
from lakehouse.tables import append, ensure_namespace

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

TABLE = "yellow_trips_bronze"
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

# TLC has renamed these across vintages; normalising early keeps silver simple.
COLUMN_ALIASES = {
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "VendorID": "vendor_id",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
}


class InvalidMonthError(ValueError):
    """Raised when a month argument is not YYYY-MM."""


def validate_month(month: str) -> str:
    """Reject anything that is not a real YYYY-MM before we hit the network."""
    if not MONTH_PATTERN.match(month):
        raise InvalidMonthError(f"month must look like YYYY-MM, got {month!r}")
    return month


def normalize_columns(df: DataFrame) -> DataFrame:
    """Apply the alias map and snake_case whatever is left over."""
    renamed = df
    for source, target in COLUMN_ALIASES.items():
        if source in renamed.columns:
            renamed = renamed.withColumnRenamed(source, target)

    for column in renamed.columns:
        snake = re.sub(r"(?<!^)(?=[A-Z])", "_", column).lower()
        if snake != column:
            renamed = renamed.withColumnRenamed(column, snake)
    return renamed


def add_audit_columns(df: DataFrame, source_uri: str, run_id: str, month: str) -> DataFrame:
    """Attach lineage columns. Bronze keeps the raw payload plus provenance."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("_source_uri", F.lit(source_uri))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_partition_month", F.lit(month))
    )


def transform(df: DataFrame, source_uri: str, run_id: str, month: str) -> DataFrame:
    """The whole bronze transform, composed. Pure and testable."""
    return add_audit_columns(normalize_columns(df), source_uri, run_id, month)


def run(spark: SparkSession, config: JobConfig, month: str, run_id: str | None = None) -> int:
    """Read one month of source parquet and append it to the bronze table.

    Returns the number of rows written.
    """
    month = validate_month(month)
    run_id = run_id or str(uuid.uuid4())
    source_uri = config.source_url(month)

    log.info("reading %s", source_uri)
    raw = spark.read.parquet(source_uri)
    bronze = transform(raw, source_uri=source_uri, run_id=run_id, month=month)

    ensure_namespace(spark, config.catalog.name, config.namespace)
    append(bronze, config.table(TABLE), partition_by=["_partition_month"])

    rows = bronze.count()
    log.info("wrote %s rows to %s (run_id=%s)", rows, config.table(TABLE), run_id)
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest NYC taxi trips into the bronze layer")
    parser.add_argument("--config", default="conf/local.yaml", help="path to a YAML job config")
    parser.add_argument("--month", required=True, help="source month as YYYY-MM")
    parser.add_argument("--master", default=None, help="Spark master override, e.g. local[*]")
    parser.add_argument("--run-id", default=None, help="override the generated run id")
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
