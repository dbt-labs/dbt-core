{% snapshot snapshot_castillo %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='"1-updated_at"',
        )
    }}
    select id,first_name,last_name,email,gender,ip_address,updated_at as "1-updated_at" from {{target.database}}.{{schema}}.seed where last_name = 'Castillo'

{% endsnapshot %}

{% snapshot snapshot_alvarez %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Alvarez'

{% endsnapshot %}


{% snapshot snapshot_kelly %}
    {# This has no target_database set, which is allowed! #}
    {{
        config(
            target_schema=schema,
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Kelly'

{% endsnapshot %}
