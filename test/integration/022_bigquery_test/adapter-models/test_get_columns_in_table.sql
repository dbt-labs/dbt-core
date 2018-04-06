

{% set source = ref('source') %}
{% set cols = adapter.get_columns_in_table(source.schema, source.name) %}

select
    {% for col in cols %}
        {{ col.name }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}

from {{ source }}
