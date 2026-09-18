-- Fail if p50 > p75 or p75 > p95 for any zone — percentiles must be monotone.
select *
from {{ ref("zone_duration_percentiles") }}
where p50_minutes > p75_minutes
   or p75_minutes > p95_minutes
