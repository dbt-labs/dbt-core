{% snapshot snap_0 %}

{{
    config(
      target_database=database,
      target_schema=schema,
      unique_key='iso3',

      strategy='timestamp',
      updated_at='snap_0_updated_at',
    )
}}

select *, current_timestamp as snap_0_updated_at from {{ ref('model_0') }}

{% endsnapshot %}