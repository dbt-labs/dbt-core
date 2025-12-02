{% snapshot my_snapshot %}
    {{
        config(
            database=var('target_database', database),
            schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{database}}.{{schema}}.seed
{% endsnapshot %}
