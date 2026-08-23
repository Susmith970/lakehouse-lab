from __future__ import annotations

from lakehouse.config import CatalogConfig, JobConfig
from lakehouse.session import ICEBERG_EXTENSIONS, spark_conf


def config(**kwargs) -> JobConfig:
    return JobConfig(
        catalog=CatalogConfig(name="lakehouse", warehouse="./warehouse"),
        namespace="taxi",
        **kwargs,
    )


def test_conf_registers_iceberg_extensions():
    assert spark_conf(config())["spark.sql.extensions"] == ICEBERG_EXTENSIONS


def test_conf_sets_default_catalog():
    assert spark_conf(config())["spark.sql.defaultCatalog"] == "lakehouse"


def test_conf_propagates_shuffle_partitions():
    assert spark_conf(config(shuffle_partitions=200))["spark.sql.shuffle.partitions"] == "200"


def test_conf_includes_catalog_options():
    conf = spark_conf(config())
    assert conf["spark.sql.catalog.lakehouse.warehouse"] == "./warehouse"


def test_conf_values_are_all_strings():
    assert all(isinstance(v, str) for v in spark_conf(config()).values())
