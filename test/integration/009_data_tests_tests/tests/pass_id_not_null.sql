
select *
from {{ ref('table_copy') }}
where id is null
