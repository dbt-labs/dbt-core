

{% set source = ref('source') %}
{% set cols = adapter.get_columns_in_table(source.schema, source.name) %}

{% set flattened = [] %}
{% for col in cols %}
    {% if col.mode == 'REPEATED' %}
        {% set _ = flattened.append(col) %}
    {% else %}
        {% set _ = flattened.extend(col.flatten()) %}
    {% endif %}
{% endfor %}

select
    {% for col in flattened %}
        {{ col.name }}
        {% if not loop.last %}, {% endif %}
    {% endfor %}

from {{ source }}
