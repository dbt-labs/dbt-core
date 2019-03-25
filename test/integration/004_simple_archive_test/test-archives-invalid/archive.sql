{% archive archive_sad %}

    {# missing target_database #}
    {{
        config(
            target_schema=schema,
            unique_key='"id" || ' ~ "'-'" ~ ' || "first_name"',
            strategy='timestamp',
            updated_at='"updated_at"',
        )
    }}
    select * from {{database}}.{{schema}}.seed

{% endarchive %}
