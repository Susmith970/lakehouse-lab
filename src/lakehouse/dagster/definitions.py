"""Top-level Dagster Definitions object.

Load with:
    dagster dev -m lakehouse.dagster
"""

from __future__ import annotations

from dagster import Definitions

from lakehouse.dagster.assets import bronze_trips, gold_trips, silver_trips
from lakehouse.dagster.resources import LakehouseConfigResource, SparkSessionResource

defs = Definitions(
    assets=[bronze_trips, silver_trips, gold_trips],
    resources={
        "lakehouse_config": LakehouseConfigResource(),
        "spark_session": SparkSessionResource(),
    },
)
