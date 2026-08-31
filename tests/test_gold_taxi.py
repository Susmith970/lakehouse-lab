from __future__ import annotations

from lakehouse.jobs.gold_taxi import zone_duration_percentiles, zone_revenue

# ---------------------------------------------------------------------------
# zone_revenue
# ---------------------------------------------------------------------------


def test_revenue_groups_by_zone(silver_trips):
    out = zone_revenue(silver_trips)
    zones = {r["pickup_location_id"] for r in out.collect()}
    assert zones == {132, 264}


def test_revenue_trip_count_is_correct(silver_trips):
    out = zone_revenue(silver_trips)
    rows = {r["pickup_location_id"]: r for r in out.collect()}
    assert rows[132]["trip_count"] == 2
    assert rows[264]["trip_count"] == 1


def test_revenue_total_is_sum_of_amounts(silver_trips):
    out = zone_revenue(silver_trips)
    rows = {r["pickup_location_id"]: r for r in out.collect()}
    assert rows[132]["total_revenue"] == round(12.5 + 9.0, 2)
    assert rows[264]["total_revenue"] == 7.25


def test_revenue_avg_is_rounded(silver_trips):
    out = zone_revenue(silver_trips)
    for row in out.collect():
        assert row["avg_revenue"] == round(row["avg_revenue"], 2)


def test_revenue_partition_month_is_preserved(silver_trips):
    out = zone_revenue(silver_trips)
    months = {r["_partition_month"] for r in out.collect()}
    assert months == {"2024-01"}


def test_revenue_output_columns(silver_trips):
    out = zone_revenue(silver_trips)
    assert set(out.columns) == {
        "pickup_location_id",
        "_partition_month",
        "trip_count",
        "total_revenue",
        "avg_revenue",
        "avg_distance_miles",
    }


# ---------------------------------------------------------------------------
# zone_duration_percentiles
# ---------------------------------------------------------------------------


def test_duration_groups_by_zone(silver_trips):
    out = zone_duration_percentiles(silver_trips)
    zones = {r["pickup_location_id"] for r in out.collect()}
    assert zones == {132, 264}


def test_duration_p50_is_positive(silver_trips):
    out = zone_duration_percentiles(silver_trips)
    for row in out.collect():
        assert row["p50_minutes"] > 0


def test_duration_percentiles_are_ordered(silver_trips):
    out = zone_duration_percentiles(silver_trips)
    for row in out.collect():
        assert row["p50_minutes"] <= row["p75_minutes"] <= row["p95_minutes"]


def test_duration_zone_132_p50_near_18_minutes(silver_trips):
    # zone 132 trips: 21 min and 15 min → median around 18
    out = zone_duration_percentiles(silver_trips)
    rows = {r["pickup_location_id"]: r for r in out.collect()}
    assert 14 <= rows[132]["p50_minutes"] <= 22


def test_duration_output_columns(silver_trips):
    out = zone_duration_percentiles(silver_trips)
    assert set(out.columns) == {
        "pickup_location_id",
        "_partition_month",
        "trip_count",
        "p50_minutes",
        "p75_minutes",
        "p95_minutes",
    }


def test_duration_trip_count_matches_revenue(silver_trips):
    rev = zone_revenue(silver_trips)
    dur = zone_duration_percentiles(silver_trips)
    rev_counts = {r["pickup_location_id"]: r["trip_count"] for r in rev.collect()}
    dur_counts = {r["pickup_location_id"]: r["trip_count"] for r in dur.collect()}
    assert rev_counts == dur_counts
