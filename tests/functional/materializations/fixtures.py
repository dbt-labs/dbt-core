fct_eph_first_sql = """
-- fct_eph_first.sql
{{ config(materialized='ephemeral') }}

with int_eph_first as(
    select * from {{ ref('int_eph_first') }}
)

select * from int_eph_first
"""

int_eph_first_sql = """
-- int_eph_first.sql
{{ config(materialized='ephemeral') }}

select
    1 as first_column,
    2 as second_column
"""

schema_yml = """
version: 2

models:
  - name: int_eph_first
    columns:
      - name: first_column
        tests:
          - not_null
      - name: second_column
        tests:
          - not_null

  - name: fct_eph_first
    columns:
      - name: first_column
        tests:
          - not_null
      - name: second_column
        tests:
          - not_null

"""

bar_sql = """
{{ config(materialized = 'table') }}

WITH foo AS (

    SELECT * FROM {{ ref('foo') }}

), foo_1 AS (

    SELECT * FROM {{ ref('foo_1') }}

), foo_2 AS (

   SELECT * FROM {{ ref('foo_2') }}

)

SELECT * FROM foo
UNION ALL
SELECT * FROM foo_1
UNION ALL
SELECT * FROM foo_2
"""

bar1_sql = """
{{ config(materialized = 'table') }}

WITH foo AS (

    SELECT * FROM {{ ref('foo') }}

), foo_1 AS (

    SELECT * FROM {{ ref('foo_1') }}

), foo_2 AS (

   SELECT * FROM {{ ref('foo_2') }}

)

SELECT * FROM foo
UNION ALL
SELECT * FROM foo_1
UNION ALL
SELECT * FROM foo_2
"""

bar2_sql = """
{{ config(materialized = 'table') }}

WITH foo AS (

    SELECT * FROM {{ ref('foo') }}

), foo_1 AS (

    SELECT * FROM {{ ref('foo_1') }}

), foo_2 AS (

   SELECT * FROM {{ ref('foo_2') }}

)

SELECT * FROM foo
UNION ALL
SELECT * FROM foo_1
UNION ALL
SELECT * FROM foo_2
"""

bar3_sql = """
{{ config(materialized = 'table') }}

WITH foo AS (

    SELECT * FROM {{ ref('foo') }}

), foo_1 AS (

    SELECT * FROM {{ ref('foo_1') }}

), foo_2 AS (

   SELECT * FROM {{ ref('foo_2') }}

)

SELECT * FROM foo
UNION ALL
SELECT * FROM foo_1
UNION ALL
SELECT * FROM foo_2
"""

bar4_sql = """
{{ config(materialized = 'table') }}

WITH foo AS (

    SELECT * FROM {{ ref('foo') }}

), foo_1 AS (

    SELECT * FROM {{ ref('foo_1') }}

), foo_2 AS (

   SELECT * FROM {{ ref('foo_2') }}

)

SELECT * FROM foo
UNION ALL
SELECT * FROM foo_1
UNION ALL
SELECT * FROM foo_2
"""

bar5_sql = """
{{ config(materialized = 'table') }}

WITH foo AS (

    SELECT * FROM {{ ref('foo') }}

), foo_1 AS (

    SELECT * FROM {{ ref('foo_1') }}

), foo_2 AS (

   SELECT * FROM {{ ref('foo_2') }}

)

SELECT * FROM foo
UNION ALL
SELECT * FROM foo_1
UNION ALL
SELECT * FROM foo_2
"""

baz_sql = """
{{ config(materialized = 'table') }}
SELECT * FROM {{ ref('bar') }}
"""

baz1_sql = """
{{ config(materialized = 'table') }}
SELECT * FROM {{ ref('bar_1') }}
"""

foo_sql = """
{{ config(materialized = 'ephemeral') }}

with source as (

    select 1 as id

), renamed as (

    select id as uid from source

)

select * from renamed
"""

foo1_sql = """
{{ config(materialized = 'ephemeral') }}

WITH source AS (

    SELECT 1 AS id

), RENAMED as (

    SELECT id as UID FROM source

)

SELECT * FROM renamed
"""

foo2_sql = """
{{ config(materialized = 'ephemeral') }}

WITH source AS (

    SELECT 1 AS id

), RENAMED as (

    SELECT id as UID FROM source

)

SELECT * FROM renamed
"""
