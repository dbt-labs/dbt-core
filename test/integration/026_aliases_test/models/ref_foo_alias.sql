
{{
    config(
        materialized='table'
    )
}}

WITH trigger_ref AS (
  SELECT
    *
  FROM
    -- we should still be able to ref a model by its filepath
    {{ ref('foo_alias') }}
)

SELECT
  -- this name should still be the filename
  '{{ this.name }}' as "tablename"
