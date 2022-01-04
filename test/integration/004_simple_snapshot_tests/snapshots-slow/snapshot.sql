
{% snapshot my_slow_snapshot %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at'
        )
    }}

    select
        id,
        updated_at,
        seconds

    from {{ ref('gen') }}

{% endsnapshot %}
