{% snapshot orders_snapshot %}

{{
    config(
      target_database='postgres',
      target_schema='snapshots',
      unique_key='sample_num',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ ref('sample_model') }}

{% endsnapshot %}
