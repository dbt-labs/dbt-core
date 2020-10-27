{% snapshot snapshot_seed %}
{{
    config(
      unique_key='id',
      strategy='check',
      check_cols='all',
      target_schema=var('alternate_schema')
    )
}}
select * from {{ ref('seed') }}
{% endsnapshot %}