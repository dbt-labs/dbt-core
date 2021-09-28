{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
        )
    }}
    select * from {{target.database}}.{{target.schema}}.seed

{% endsnapshot %}

{% snapshot snapshot_castillo %}

    {{
        config(
            target_database=var('target_database', database),
            updated_at='"1-updated_at"',
        )
    }}
    select id,first_name,last_name,email,gender,ip_address,updated_at as "1-updated_at" from {{target.database}}.{{schema}}.seed where last_name = 'Castillo'

{% endsnapshot %}

{% snapshot snapshot_alvarez %}

    {{
        config(
            target_database=var('target_database', database),
        )
    }}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Alvarez'

{% endsnapshot %}


{% snapshot snapshot_kelly %}
    {# This has no target_database set, which is allowed! #}
    select * from {{target.database}}.{{schema}}.seed where last_name = 'Kelly'

{% endsnapshot %}
