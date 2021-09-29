
-- This model checks to confirm that each date partition was created correctly.
-- Columns day_1, day_2, and day_3 should have a value of 1, and count_days should be 3

with base as (

  select
    case when _PARTITIONTIME = '2018-01-01' then 1 else 0 end as day_1,
    case when _PARTITIONTIME = '2018-01-02' then 1 else 0 end as day_2,
    case when _PARTITIONTIME = '2018-01-03' then 1 else 0 end as day_3
  from {{ ref('partitioned') }}

)

select distinct
  sum(day_1) over () as day_1,
  sum(day_2) over () as day_2,
  sum(day_3) over () as day_3,
  count(*) over () as count_days
from base
