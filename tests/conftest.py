"""Shared pytest fixtures.

The session here is deliberately plain -- no Iceberg extensions, no catalog.
Transforms are pure DataFrame functions, so they can be tested without pulling
jars or hitting a warehouse, which keeps CI to seconds instead of minutes.
"""

from __future__ import annotations

from datetime import datetime

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession, skipped when pyspark is not installed."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("lakehouse-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def raw_trips(spark):
    """A miniature TLC-shaped frame using the original vendor column names."""
    rows = [
        (1, datetime(2024, 1, 2, 8, 30), datetime(2024, 1, 2, 8, 51), 132, 264, 12.5),
        (2, datetime(2024, 1, 2, 9, 5), datetime(2024, 1, 2, 9, 17), 264, 132, 7.25),
    ]
    return spark.createDataFrame(
        rows,
        schema=(
            "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
            "PULocationID int, DOLocationID int, total_amount double"
        ),
    )
