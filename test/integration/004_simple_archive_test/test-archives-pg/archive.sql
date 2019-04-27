{% archive archive_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{database}}.{{schema}}.seed

{% endarchive %}
