select
*
from {{ ref('base_table') }} as base
full join {{ ref('data_seed_extended') }} as seed
on base.id = seed.id
where base.id is null or seed.id is null