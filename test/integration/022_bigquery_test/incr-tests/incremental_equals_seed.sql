select
*
from {{ ref('incremental') }} as incr
full join {{ ref('data_seed') }} as seed
on incr.id = seed.id
where incr.id is null or seed.id is null