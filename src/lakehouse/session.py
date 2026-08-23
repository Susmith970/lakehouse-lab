"""SparkSession construction.

Jobs never build a session inline -- they ask for one here so that Iceberg
extensions, catalog registration and tuning stay consistent across every entry
point (CLI, tests, notebooks, Dagster ops).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from lakehouse.config import ICEBERG_RUNTIME, JobConfig

if TYPE_CHECKING:  # pragma: no cover - import cost only matters at runtime
    from pyspark.sql import SparkSession

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def spark_conf(config: JobConfig) -> dict[str, str]:
    """Full Spark configuration for a job. Pure -- safe to assert on in tests."""
    conf = {
        "spark.sql.extensions": ICEBERG_EXTENSIONS,
        "spark.sql.defaultCatalog": config.catalog.name,
        "spark.sql.shuffle.partitions": str(config.shuffle_partitions),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.jars.packages": ICEBERG_RUNTIME,
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }
    conf.update(config.catalog.spark_options())
    return conf


def build_session(config: JobConfig, master: str | None = None) -> SparkSession:
    """Create (or reuse) a SparkSession wired for Iceberg."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(config.app_name)
    if master:
        builder = builder.master(master)
    for key, value in spark_conf(config).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


@contextmanager
def session_scope(config: JobConfig, master: str | None = None) -> Iterator[SparkSession]:
    """Session as a context manager so CLI jobs always stop cleanly."""
    spark = build_session(config, master=master)
    try:
        yield spark
    finally:
        spark.stop()
