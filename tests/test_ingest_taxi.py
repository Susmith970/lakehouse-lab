from __future__ import annotations

import pytest

from lakehouse.jobs.ingest_taxi import (
    InvalidMonthError,
    add_audit_columns,
    normalize_columns,
    transform,
    validate_month,
)


@pytest.mark.parametrize("month", ["2024-01", "1999-12", "2030-09"])
def test_validate_month_accepts_real_months(month):
    assert validate_month(month) == month


@pytest.mark.parametrize("month", ["2024-13", "2024-00", "202401", "jan-2024", ""])
def test_validate_month_rejects_junk(month):
    with pytest.raises(InvalidMonthError):
        validate_month(month)


def test_normalize_columns_applies_alias_map(raw_trips):
    columns = normalize_columns(raw_trips).columns
    assert "pickup_ts" in columns
    assert "dropoff_ts" in columns
    assert "tpep_pickup_datetime" not in columns


def test_normalize_columns_snake_cases_leftovers(raw_trips):
    columns = normalize_columns(raw_trips).columns
    assert "vendor_id" in columns
    assert "pickup_location_id" in columns
    assert not any(c != c.lower() for c in columns)


def test_normalize_columns_preserves_row_count(raw_trips):
    assert normalize_columns(raw_trips).count() == raw_trips.count()


def test_normalize_columns_is_idempotent(raw_trips):
    once = normalize_columns(raw_trips)
    assert normalize_columns(once).columns == once.columns


def test_audit_columns_are_added(raw_trips):
    out = add_audit_columns(raw_trips, "s3://src/file.parquet", "run-1", "2024-01")
    for column in ("_source_uri", "_run_id", "_ingested_at", "_partition_month"):
        assert column in out.columns


def test_audit_columns_carry_the_given_values(raw_trips):
    row = add_audit_columns(raw_trips, "s3://src/f.parquet", "run-1", "2024-01").first()
    assert row["_source_uri"] == "s3://src/f.parquet"
    assert row["_run_id"] == "run-1"
    assert row["_partition_month"] == "2024-01"


def test_transform_composes_both_stages(raw_trips):
    out = transform(raw_trips, "s3://src/f.parquet", "run-1", "2024-01")
    assert "pickup_ts" in out.columns
    assert "_run_id" in out.columns
    assert out.count() == raw_trips.count()
