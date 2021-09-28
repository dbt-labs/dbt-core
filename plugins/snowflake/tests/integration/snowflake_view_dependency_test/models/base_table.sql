
{{ config(materialized='table') }}

select *
    {% if var('add_table_field', False) %}
        , 1 as new_field
    {% endif %}

from {{ ref('people') }}
