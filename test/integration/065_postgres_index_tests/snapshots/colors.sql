{% snapshot colors %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color'],
            indexes=[
              {'columns': ['id'], 'type': 'hash'},
              {'columns': ['id', 'color'], 'unique': True},
            ]
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color union all
        select 2 as id, 'green' as color

    {% else %}

        select 1 as id, 'blue' as color union all
        select 2 as id, 'green' as color

    {% endif %}

{% endsnapshot %}
