from __future__ import annotations

import pytest

from lakehouse.quality import (
    QualityError,
    Violation,
    enforce,
    no_future_timestamps,
    not_empty,
    null_rate,
    value_range,
)

# ---------------------------------------------------------------------------
# not_empty
# ---------------------------------------------------------------------------


def test_not_empty_passes_on_nonempty_frame(bronze_trips):
    assert not_empty()(bronze_trips) is None


def test_not_empty_fails_on_zero_rows(spark):
    empty = spark.createDataFrame([], schema="vendor_id int")
    v = not_empty()(empty)
    assert v is not None
    assert "zero rows" in v.detail


# ---------------------------------------------------------------------------
# null_rate
# ---------------------------------------------------------------------------


def test_null_rate_passes_when_under_threshold(spark):
    df = spark.createDataFrame([(1,), (2,), (None,)], schema="x int")
    assert null_rate("x", max_rate=0.5)(df) is None


def test_null_rate_fails_when_over_threshold(spark):
    df = spark.createDataFrame([(1,), (None,), (None,)], schema="x int")
    v = null_rate("x", max_rate=0.5)(df)
    assert v is not None
    assert "null_rate(x)" in v.check


def test_null_rate_passes_on_empty_frame(spark):
    empty = spark.createDataFrame([], schema="x int")
    assert null_rate("x", max_rate=0.0)(empty) is None


def test_null_rate_default_threshold_is_zero(spark):
    df = spark.createDataFrame([(1,), (None,)], schema="x int")
    v = null_rate("x")(df)
    assert v is not None


# ---------------------------------------------------------------------------
# value_range
# ---------------------------------------------------------------------------


def test_value_range_passes_when_all_in_bounds(spark):
    df = spark.createDataFrame([(1.0,), (2.0,), (3.0,)], schema="x double")
    assert value_range("x", lo=0.0, hi=10.0)(df) is None


def test_value_range_fails_on_value_below_lo(spark):
    df = spark.createDataFrame([(1.0,), (-1.0,)], schema="x double")
    v = value_range("x", lo=0.0)(df)
    assert v is not None
    assert "value_range(x)" in v.check
    assert "1 row" in v.detail


def test_value_range_fails_on_value_above_hi(spark):
    df = spark.createDataFrame([(5.0,), (100.0,)], schema="x double")
    v = value_range("x", hi=10.0)(df)
    assert v is not None


def test_value_range_one_sided_lo_only(spark):
    df = spark.createDataFrame([(0.0,), (5.0,)], schema="x double")
    assert value_range("x", lo=0.0)(df) is None


def test_value_range_counts_all_violations(spark):
    df = spark.createDataFrame([(-1.0,), (-2.0,), (1.0,)], schema="x double")
    v = value_range("x", lo=0.0)(df)
    assert v is not None
    assert "2 row" in v.detail


# ---------------------------------------------------------------------------
# no_future_timestamps
# ---------------------------------------------------------------------------


def test_no_future_timestamps_passes_on_past_dates(bronze_trips):
    assert no_future_timestamps("pickup_ts")(bronze_trips) is None


def test_no_future_timestamps_fails_on_future_date(spark):
    from datetime import datetime

    df = spark.createDataFrame([(datetime(2099, 1, 1, 0, 0),)], schema="ts timestamp")
    v = no_future_timestamps("ts")(df)
    assert v is not None
    assert "future" in v.detail


# ---------------------------------------------------------------------------
# enforce
# ---------------------------------------------------------------------------


def test_enforce_passes_silently_when_all_checks_pass(bronze_trips):
    enforce(bronze_trips, [not_empty()])  # should not raise


def test_enforce_raises_quality_error_on_failure(spark):
    empty = spark.createDataFrame([], schema="x int")
    with pytest.raises(QualityError) as exc_info:
        enforce(empty, [not_empty()])
    assert len(exc_info.value.violations) == 1


def test_enforce_collects_all_violations_before_raising(spark):
    df = spark.createDataFrame([(-1.0,)], schema="x double")
    with pytest.raises(QualityError) as exc_info:
        enforce(
            df,
            [
                value_range("x", lo=0.0),
                value_range("x", lo=0.0, hi=0.5),
            ],
        )
    assert len(exc_info.value.violations) == 2


def test_quality_error_message_lists_violations(spark):
    empty = spark.createDataFrame([], schema="x int")
    with pytest.raises(QualityError) as exc_info:
        enforce(empty, [not_empty()])
    assert "zero rows" in str(exc_info.value)


def test_violation_str_includes_check_and_detail():
    v = Violation(check="my_check", detail="something went wrong")
    assert "my_check" in str(v)
    assert "something went wrong" in str(v)
