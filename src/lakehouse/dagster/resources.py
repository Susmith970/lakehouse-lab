"""Dagster resources: config loader and Spark session."""

from __future__ import annotations

from contextlib import contextmanager

from dagster import ConfigurableResource

from lakehouse.config import JobConfig, load_config
from lakehouse.session import session_scope


class LakehouseConfigResource(ConfigurableResource):
    config_path: str = "conf/local.yaml"

    def load(self) -> JobConfig:
        return load_config(self.config_path)


class SparkSessionResource(ConfigurableResource):
    master: str = "local[*]"

    @contextmanager
    def get_session(self, config: JobConfig):
        with session_scope(config, master=self.master) as spark:
            yield spark
