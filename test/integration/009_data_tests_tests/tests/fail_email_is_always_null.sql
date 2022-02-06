
select *
from {{ ref('table_copy') }}
where email is not null
