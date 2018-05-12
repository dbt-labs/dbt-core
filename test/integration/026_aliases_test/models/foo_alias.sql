
{{
    config(
        alias='foo',
        materialized='table'
    )
}}

SELECT
  '{{ this.alias }}'   as "tablename"
