- add a comment
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

{% snapshot orders2_snapshot %}

{{
    config(
      target_schema=schema,
      strategy='check',
      unique_key='id',
      check_cols=['order_date'],
    )
}}

select * from {{ ref('orders') }}

{% endsnapshot %}
