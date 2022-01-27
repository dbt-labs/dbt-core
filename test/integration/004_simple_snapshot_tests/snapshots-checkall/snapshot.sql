{% snapshot my_snapshot %}
    {{ config(check_cols='all', unique_key='id', strategy='check', target_database=database, target_schema=schema) }}
    select * from {{ ref(var('seed_name', 'seed')) }}
{% endsnapshot %}
