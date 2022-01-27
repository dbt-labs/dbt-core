{% snapshot snap_99 %}

{{
    config(
      target_database=database,
      target_schema=schema,
      strategy='timestamp',
      unique_key='num',
      updated_at='snap_99_updated_at',
    )
}}

select *, current_timestamp as snap_99_updated_at from {{ ref('model_99') }}

{% endsnapshot %}