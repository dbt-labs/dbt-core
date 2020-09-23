{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}

    {% if var('invalidate_hard_deletes', 'false') | as_bool %}
        {{ config(invalidate_hard_deletes=True) }}
    {% endif %}

    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}
