{%- if target.type == 'bigquery' -%}
  {{ config(project=var('alternate_db')) }}
{%- else -%}
  {{ config(database=var('alternate_db')) }}
{%- endif -%}
select * from {{ ref('seed') }}
