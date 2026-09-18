{{
    config(
        materialized="table",
        description="Total and average revenue per pickup zone per month. Query target for BI dashboards."
    )
}}

select
    pickup_location_id,
    _partition_month,
    count(*)                                  as trip_count,
    round(sum(total_amount), 2)               as total_revenue,
    round(avg(total_amount), 2)               as avg_revenue,
    round(avg(trip_distance), 2)              as avg_distance_miles
from {{ ref("stg_silver_trips") }}
group by pickup_location_id, _partition_month
