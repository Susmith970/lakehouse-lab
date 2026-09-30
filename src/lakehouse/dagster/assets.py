"""Software-defined assets wrapping the three pipeline jobs.

Each asset delegates entirely to the existing run() function — no logic lives
here. The dependency chain (bronze -> silver -> gold) is declared via deps so
Dagster can materialise them in order and show lineage in the UI.
"""

from __future__ import annotations

from dagster import AssetExecutionContext, Config, asset

from lakehouse.dagster.resources import LakehouseConfigResource, SparkSessionResource
from lakehouse.jobs import gold_taxi, ingest_taxi, silver_taxi


class MonthConfig(Config):
    month: str = "2024-01"


@asset(group_name="lakehouse")
def bronze_trips(
    context: AssetExecutionContext,
    config: MonthConfig,
    lakehouse_config: LakehouseConfigResource,
    spark_session: SparkSessionResource,
) -> None:
    job_config = lakehouse_config.load()
    with spark_session.get_session(job_config) as spark:
        rows = ingest_taxi.run(spark, job_config, month=config.month)
    context.log.info("bronze_trips: wrote %d rows for %s", rows, config.month)


@asset(deps=["bronze_trips"], group_name="lakehouse")
def silver_trips(
    context: AssetExecutionContext,
    config: MonthConfig,
    lakehouse_config: LakehouseConfigResource,
    spark_session: SparkSessionResource,
) -> None:
    job_config = lakehouse_config.load()
    with spark_session.get_session(job_config) as spark:
        rows = silver_taxi.run(spark, job_config, month=config.month)
    context.log.info("silver_trips: wrote %d rows for %s", rows, config.month)


@asset(deps=["silver_trips"], group_name="lakehouse")
def gold_trips(
    context: AssetExecutionContext,
    config: MonthConfig,
    lakehouse_config: LakehouseConfigResource,
    spark_session: SparkSessionResource,
) -> None:
    job_config = lakehouse_config.load()
    with spark_session.get_session(job_config) as spark:
        counts = gold_taxi.run(spark, job_config, month=config.month)
    context.log.info(
        "gold_trips: wrote %d revenue rows, %d duration rows for %s",
        counts["revenue_rows"],
        counts["duration_rows"],
        config.month,
    )
