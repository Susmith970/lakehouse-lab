-- Fail if any zone has negative total revenue.
-- dbt singular tests return rows on failure; zero rows = pass.
select *
from {{ ref("zone_revenue") }}
where total_revenue < 0
