"""Iceberg table helpers.

Thin wrappers over Spark SQL / the DataFrameWriterV2 API. They exist so jobs
express intent ("append to bronze", "replace this partition") instead of
repeating writer boilerplate.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


def ensure_namespace(spark: SparkSession, catalog: str, namespace: str) -> None:
    """Create the namespace if it does not exist yet."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def table_exists(spark: SparkSession, table: str) -> bool:
    """True if the Iceberg table is already registered in the catalog."""
    try:
        return spark.catalog.tableExists(table)
    except Exception:  # noqa: BLE001 - catalog impls raise a variety of types
        log.debug("tableExists check failed for %s", table, exc_info=True)
        return False


def append(
    df: DataFrame,
    table: str,
    partition_by: Sequence[str] | None = None,
    sort_by: Sequence[str] | None = None,
) -> None:
    """Append to ``table``, creating it on first write.

    Partitioning and sort order are only applied at creation time, which
    matches Iceberg semantics: layout changes afterwards are an explicit
    ALTER TABLE, not a silent side effect of a write.
    """
    writer = df.writeTo(table)
    if not table_exists(df.sparkSession, table):
        if partition_by:
            from pyspark.sql import functions as F

            writer = writer.partitionedBy(*[F.col(c) for c in partition_by])
        if sort_by:
            writer = writer.tableProperty("sort-order", ", ".join(sort_by))
        log.info("creating table %s", table)
        writer.create()
        return
    log.info("appending to table %s", table)
    writer.append()


def overwrite_partitions(df: DataFrame, table: str) -> None:
    """Dynamic partition overwrite -- the idempotent path for backfills."""
    df.writeTo(table).overwritePartitions()


def snapshot_count(spark: SparkSession, table: str) -> int:
    """Number of snapshots retained for a table. Handy in tests and audits."""
    return spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").collect()[0]["n"]
