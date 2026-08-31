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
def bronze_trips(spark):
    """Normalised-column frame that mirrors bronze output, including invalid rows."""
    rows = [
        # two valid, distinct trips
        (1, datetime(2024, 1, 2, 8, 30), datetime(2024, 1, 2, 8, 51), 132, 264, 1, 2.3, 10.0, 12.5),
        (2, datetime(2024, 1, 2, 9, 5), datetime(2024, 1, 2, 9, 17), 264, 132, 2, 1.1, 5.5, 7.25),
        # exact duplicate of the first valid trip
        (1, datetime(2024, 1, 2, 8, 30), datetime(2024, 1, 2, 8, 51), 132, 264, 1, 2.3, 10.0, 12.5),
        # invalid: trip_distance is zero
        (1, datetime(2024, 1, 3, 10, 0), datetime(2024, 1, 3, 10, 15), 100, 200, 1, 0.0, 4.0, 5.0),
        # invalid: pickup after dropoff
        (2, datetime(2024, 1, 3, 11, 0), datetime(2024, 1, 3, 10, 45), 100, 200, 1, 2.0, 4.0, 5.0),
        # invalid: passenger_count is zero
        (1, datetime(2024, 1, 4, 8, 0), datetime(2024, 1, 4, 8, 30), 132, 264, 0, 3.0, 7.0, 8.0),
    ]
    return spark.createDataFrame(
        rows,
        schema=(
            "vendor_id int, pickup_ts timestamp, dropoff_ts timestamp,"
            " pickup_location_id int, dropoff_location_id int,"
            " passenger_count int, trip_distance double,"
            " fare_amount double, total_amount double"
        ),
    )


@pytest.fixture
def silver_trips(spark):
    """Clean silver-shaped frame: valid trips across two zones."""
    rows = [
        # zone 132 — two trips, 21 min and 15 min
        (132, datetime(2024, 1, 2, 8, 30), datetime(2024, 1, 2, 8, 51), 12.5, 2.3, "2024-01"),
        (132, datetime(2024, 1, 3, 9, 0), datetime(2024, 1, 3, 9, 15), 9.0, 1.8, "2024-01"),
        # zone 264 — one trip, 12 min
        (264, datetime(2024, 1, 2, 9, 5), datetime(2024, 1, 2, 9, 17), 7.25, 1.1, "2024-01"),
    ]
    return spark.createDataFrame(
        rows,
        schema=(
            "pickup_location_id int, pickup_ts timestamp, dropoff_ts timestamp,"
            " total_amount double, trip_distance double, _partition_month string"
        ),
    )


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
