select
*
from {{ ref('unique') }} as uniq
full join {{ ref('data_seed') }} as seed
on uniq.id = seed.id
where uniq.id is null or seed.id is null