{% macro redshift__get_show_grant_sql(relation) %}

with privileges as (

    -- valid options per https://docs.aws.amazon.com/redshift/latest/dg/r_HAS_TABLE_PRIVILEGE.html
    select 'select' as privilege_type
    union all
    select 'insert' as privilege_type
    union all
    select 'update' as privilege_type
    union all
    select 'delete' as privilege_type
    union all
    select 'references' as privilege_type

)

select
    u.usename as grantee,
    p.privilege_type
from pg_user u
cross join privileges p
where has_table_privilege(u.usename, '{{ relation }}', privilege_type)
    and u.usename != current_user
    and not u.usesuper

{% endmacro %}
