
{% if var('dependent_type', 'view') == 'view' %}
    {{ config(materialized='view') }}
{% else %}
    {{ config(materialized='table') }}
{% endif %}

select * from {{ ref('base_table') }}
