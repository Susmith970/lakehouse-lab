from __future__ import annotations

import pytest

from lakehouse.jobs.silver_taxi import (
    DEDUP_KEY,
    add_silver_columns,
    apply_quality_rules,
    cast_types,
    deduplicate,
    transform,
)


# ---------------------------------------------------------------------------
# cast_types
# ---------------------------------------------------------------------------


def test_cast_types_preserves_row_count(bronze_trips):
    assert cast_types(bronze_trips).count() == bronze_trips.count()


def test_cast_types_vendor_id_is_int(bronze_trips):
    from pyspark.sql.types import IntegerType

    field = {f.name: f for f in cast_types(bronze_trips).schema.fields}
    assert isinstance(field["vendor_id"].dataType, IntegerType)


def test_cast_types_amounts_are_double(bronze_trips):
    from pyspark.sql.types import DoubleType

    fields = {f.name: f for f in cast_types(bronze_trips).schema.fields}
    assert isinstance(fields["trip_distance"].dataType, DoubleType)
    assert isinstance(fields["total_amount"].dataType, DoubleType)


# ---------------------------------------------------------------------------
# apply_quality_rules
# ---------------------------------------------------------------------------


def test_quality_rules_drop_zero_distance(bronze_trips):
    valid = apply_quality_rules(cast_types(bronze_trips))
    distances = [r["trip_distance"] for r in valid.collect()]
    assert all(d > 0 for d in distances)


def test_quality_rules_drop_inverted_timestamps(bronze_trips):
    valid = apply_quality_rules(cast_types(bronze_trips))
    for row in valid.collect():
        assert row["pickup_ts"] < row["dropoff_ts"]


def test_quality_rules_drop_zero_passengers(bronze_trips):
    valid = apply_quality_rules(cast_types(bronze_trips))
    counts = [r["passenger_count"] for r in valid.collect()]
    assert all(c >= 1 for c in counts)


def test_quality_rules_passes_valid_rows(bronze_trips):
    # fixture has 2 distinct valid rows + 1 duplicate + 3 invalid
    valid = apply_quality_rules(cast_types(bronze_trips))
    assert valid.count() == 3  # 2 valid + 1 duplicate still present here


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------


def test_deduplicate_removes_exact_dupes(bronze_trips):
    # after quality filter we have 3 rows (2 valid + 1 dup); dedup brings it to 2
    filtered = apply_quality_rules(cast_types(bronze_trips))
    assert deduplicate(filtered).count() == 2


def test_deduplicate_key_columns_all_present(bronze_trips):
    deduped = deduplicate(bronze_trips)
    for col in DEDUP_KEY:
        assert col in deduped.columns


def test_deduplicate_is_idempotent(bronze_trips):
    once = deduplicate(bronze_trips)
    assert deduplicate(once).count() == once.count()


# ---------------------------------------------------------------------------
# add_silver_columns
# ---------------------------------------------------------------------------


def test_silver_columns_are_added(bronze_trips):
    out = add_silver_columns(bronze_trips, "run-42", "2024-01")
    for col in ("_silver_run_id", "_silver_at", "_partition_month"):
        assert col in out.columns


def test_silver_columns_carry_correct_values(bronze_trips):
    row = add_silver_columns(bronze_trips, "run-42", "2024-01").first()
    assert row["_silver_run_id"] == "run-42"
    assert row["_partition_month"] == "2024-01"


# ---------------------------------------------------------------------------
# transform (integration of all stages)
# ---------------------------------------------------------------------------


def test_transform_produces_only_valid_unique_rows(bronze_trips):
    out = transform(bronze_trips, run_id="r1", month="2024-01")
    assert out.count() == 2


def test_transform_output_has_silver_lineage(bronze_trips):
    out = transform(bronze_trips, run_id="r1", month="2024-01")
    assert "_silver_run_id" in out.columns
    assert "_silver_at" in out.columns


def test_transform_preserves_trip_columns(bronze_trips):
    out = transform(bronze_trips, run_id="r1", month="2024-01")
    for col in ("pickup_ts", "dropoff_ts", "total_amount", "trip_distance"):
        assert col in out.columns
