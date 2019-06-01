{% archive archive_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='concat(cast(id as string) , "-", first_name)',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from `{{target.database}}`.`{{schema}}`.seed

{% endarchive %}
