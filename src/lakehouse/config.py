"""Typed configuration for lakehouse jobs.

Config is layered: YAML file first, then environment overrides prefixed with
``LAKEHOUSE_``. Keeping this in one place means jobs never read os.environ
directly, which makes them trivial to unit test.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ENV_PREFIX = "LAKEHOUSE_"

ICEBERG_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

_CATALOG_IMPLS = {
    "hadoop": "org.apache.iceberg.hadoop.HadoopCatalog",
    "glue": "org.apache.iceberg.aws.glue.GlueCatalog",
}


class ConfigError(ValueError):
    """Raised when a config file is missing or holds invalid values."""


@dataclass(frozen=True)
class CatalogConfig:
    """Iceberg catalog settings.

    ``type`` is one of ``hadoop`` (local dev), ``glue`` (AWS), or ``rest``.
    """

    name: str
    warehouse: str
    type: str = "hadoop"
    uri: str | None = None

    def __post_init__(self) -> None:
        if self.type not in {"hadoop", "glue", "rest"}:
            raise ConfigError(f"unsupported catalog type: {self.type!r}")
        if self.type == "rest" and not self.uri:
            raise ConfigError("catalog type 'rest' requires a uri")

    def spark_options(self) -> dict[str, str]:
        """Spark conf entries that register this catalog."""
        prefix = f"spark.sql.catalog.{self.name}"
        opts = {
            prefix: "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.warehouse": self.warehouse,
        }
        if self.type == "rest":
            opts[f"{prefix}.type"] = "rest"
            opts[f"{prefix}.uri"] = self.uri or ""
        else:
            opts[f"{prefix}.catalog-impl"] = _CATALOG_IMPLS[self.type]
        if self.type == "glue":
            opts[f"{prefix}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
        return opts


@dataclass(frozen=True)
class JobConfig:
    """Everything a job needs to run."""

    catalog: CatalogConfig
    namespace: str
    app_name: str = "lakehouse-lab"
    shuffle_partitions: int = 8
    source_url_template: str = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
    )

    def table(self, name: str) -> str:
        """Fully qualified Iceberg table identifier."""
        return f"{self.catalog.name}.{self.namespace}.{name}"

    def source_url(self, month: str) -> str:
        return self.source_url_template.format(month=month)


def _coerce(raw: str) -> Any:
    lowered = raw.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(raw)
    except ValueError:
        return raw


def _env_overrides(env: dict[str, str]) -> dict[str, Any]:
    """Turn LAKEHOUSE_CATALOG__WAREHOUSE=/tmp into {'catalog': {'warehouse': '/tmp'}}."""
    out: dict[str, Any] = {}
    for key, value in env.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = key[len(ENV_PREFIX) :].lower().split("__")
        cursor = out
        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[path[-1]] = _coerce(value)
    return out


def _merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | Path, env: dict[str, str] | None = None) -> JobConfig:
    """Load a YAML config file and apply environment overrides."""
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")

    raw = yaml.safe_load(path.read_text()) or {}
    raw = _merge(raw, _env_overrides(dict(os.environ) if env is None else dict(env)))

    if "catalog" not in raw:
        raise ConfigError("config is missing the 'catalog' section")

    try:
        catalog = CatalogConfig(**dict(raw["catalog"]))
    except TypeError as exc:
        raise ConfigError(f"invalid catalog config: {exc}") from exc

    job_keys = {"namespace", "app_name", "shuffle_partitions", "source_url_template"}
    kwargs = {k: v for k, v in raw.items() if k in job_keys}
    if "namespace" not in kwargs:
        raise ConfigError("config is missing 'namespace'")

    return JobConfig(catalog=catalog, **kwargs)
