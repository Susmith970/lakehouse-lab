{{
    config(
        materialized="table",
        description="p50/p75/p95 trip duration in minutes per pickup zone per month."
    )
}}

select
    pickup_location_id,
    _partition_month,
    count(*)                                                    as trip_count,
    round(percentile_approx(duration_minutes, 0.50), 1)         as p50_minutes,
    round(percentile_approx(duration_minutes, 0.75), 1)         as p75_minutes,
    round(percentile_approx(duration_minutes, 0.95), 1)         as p95_minutes
from {{ ref("stg_silver_trips") }}
group by pickup_location_id, _partition_month
