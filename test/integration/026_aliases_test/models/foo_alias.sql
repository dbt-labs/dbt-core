
{{
    config(
        alias='foo',
        materialized='table'
    )
}}

SELECT
  '{{ this.name }}'   as "tablename"
