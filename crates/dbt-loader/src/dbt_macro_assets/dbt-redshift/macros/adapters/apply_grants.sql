{% macro redshift__get_show_grant_sql(relation) %}
  {% if redshift__use_show_apis() %}
{#-
    Use SHOW GRANTS for cross-database support (required for RA3/datasharing).
    Note: SHOW GRANTS conflates groups and roles — groups appear with a '/'
    prefix on identity_name and identity_type='role'. The standardize_grants_dict
    method in RedshiftAdapter handles this translation.
-#}
    SHOW GRANTS ON TABLE {{ adapter.quote(relation.database) }}.{{ adapter.quote(relation.schema) }}.{{ adapter.quote(relation.identifier) }}
  {% else %}
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
  {% endif %}
{% endmacro %}
