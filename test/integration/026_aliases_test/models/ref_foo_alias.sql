
{{
    config(
        materialized='table'
    )
}}

WITH trigger_ref AS (
  SELECT
    *
  FROM
    -- we should by able to still ref a model by it's filepath
    {{ ref('foo_alias') }}
)

SELECT
  -- this name should still the filename
  '{{ this.alias }}' as "tablename"