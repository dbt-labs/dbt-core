{% archive archive_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='check',
            check_cols=['email'],
        )
    }}
    select * from {{target.database}}.{{schema}}.seed

{% endarchive %}

{# This should be exactly the same #}
{% archive archive_checkall %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='check',
            check_cols='all',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed
{% endarchive %}
