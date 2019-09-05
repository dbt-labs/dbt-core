{% snapshot good_snapshot %}
    {{ config(target_schema=schema, target_database=database, strategy='timestamp', unique_key='id', updated_at='updated_at_not_real')}}
    select * from {{ schema }}.good
{% endsnapshot %}
