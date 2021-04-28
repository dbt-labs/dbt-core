{# Same as `pass_id_not_null` but with dots in its name #}

select *
from {{ ref('table_copy') }}
where id is null
