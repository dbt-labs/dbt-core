{% snapshot orders_snapshot %}

{{
    config(
      target_schema=schema,
      strategy='check',
      unique_key='id',
      check_cols=['status'],
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}

