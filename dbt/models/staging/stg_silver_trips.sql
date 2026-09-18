{{
    config(
        materialized="view",
        description="Staging view over the silver Iceberg table. Selects the columns downstream models need and casts duration to minutes."
    )
}}

select
    vendor_id,
    pickup_ts,
    dropoff_ts,
    pickup_location_id,
    dropoff_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    _partition_month,

    -- derived
    round(
        (unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60.0,
        1
    ) as duration_minutes

from {{ source("lakehouse", "yellow_trips_silver") }}
