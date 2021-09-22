{%- if adapter.type() == 'snowflake' -%}
    {%- set schema_suffix = 'TEST' -%}
{%- else -%}
    {%- set schema_suffix = 'test' -%}
{%- endif -%}
{{
    config(
        materialized='view',
        schema=schema_suffix,
    )
}}

select * from {{ ref('seed') }}
