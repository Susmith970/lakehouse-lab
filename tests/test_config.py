from __future__ import annotations

import pytest

from lakehouse.config import CatalogConfig, ConfigError, load_config

LOCAL_YAML = """
namespace: taxi
app_name: test-app
catalog:
  name: lakehouse
  type: hadoop
  warehouse: ./warehouse
"""


def write(tmp_path, body, name="job.yaml"):
    path = tmp_path / name
    path.write_text(body)
    return path


def test_loads_yaml(tmp_path):
    config = load_config(write(tmp_path, LOCAL_YAML), env={})
    assert config.namespace == "taxi"
    assert config.app_name == "test-app"
    assert config.catalog.warehouse == "./warehouse"


def test_table_identifier_is_fully_qualified(tmp_path):
    config = load_config(write(tmp_path, LOCAL_YAML), env={})
    assert config.table("yellow_trips_bronze") == "lakehouse.taxi.yellow_trips_bronze"


def test_env_overrides_nested_keys(tmp_path):
    config = load_config(
        write(tmp_path, LOCAL_YAML),
        env={
            "LAKEHOUSE_CATALOG__WAREHOUSE": "s3://bucket/warehouse",
            "LAKEHOUSE_SHUFFLE_PARTITIONS": "200",
        },
    )
    assert config.catalog.warehouse == "s3://bucket/warehouse"
    assert config.shuffle_partitions == 200


def test_env_overrides_ignore_unrelated_vars(tmp_path):
    config = load_config(write(tmp_path, LOCAL_YAML), env={"PATH": "/usr/bin"})
    assert config.catalog.warehouse == "./warehouse"


def test_missing_file_raises(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yaml", env={})


def test_missing_catalog_section_raises(tmp_path):
    with pytest.raises(ConfigError, match="catalog"):
        load_config(write(tmp_path, "namespace: taxi\n"), env={})


def test_source_url_interpolates_month(tmp_path):
    config = load_config(write(tmp_path, LOCAL_YAML), env={})
    assert config.source_url("2024-01").endswith("yellow_tripdata_2024-01.parquet")


def test_hadoop_catalog_spark_options():
    opts = CatalogConfig(name="lh", warehouse="./w").spark_options()
    assert opts["spark.sql.catalog.lh.catalog-impl"].endswith("HadoopCatalog")
    assert opts["spark.sql.catalog.lh.warehouse"] == "./w"


def test_glue_catalog_uses_s3_file_io():
    opts = CatalogConfig(name="lh", warehouse="s3://b/w", type="glue").spark_options()
    assert opts["spark.sql.catalog.lh.io-impl"].endswith("S3FileIO")


def test_rest_catalog_requires_uri():
    with pytest.raises(ConfigError, match="uri"):
        CatalogConfig(name="lh", warehouse="s3://b/w", type="rest")


def test_unknown_catalog_type_rejected():
    with pytest.raises(ConfigError, match="unsupported"):
        CatalogConfig(name="lh", warehouse="./w", type="hive")
