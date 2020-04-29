{# If our source override didn't take, this would be an errror #}
select * from {{ source('my_source', 'my_table') }}
