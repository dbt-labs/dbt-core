-- funcsign: (relation, list[string]) -> string
{% macro default__test_aggregated_not_null(model, column_names) %}
{% set skip_column_names = aggregated_test_skip_column_names | default([]) %}
{% set filtered_columns = [] %}

{% for column_name in column_names %}
    {% if column_name not in skip_column_names %}
        {% do filtered_columns.append(column_name) %}
    {% endif %}
{% endfor %}
{% set union_queries = [] %}

{% for column_name in filtered_columns %}
    {% set query %}
    select
        {{ dbt.string_literal(column_name) }} as column_name
    from {{ model }}
    where {{ column_name }} is null
    {% endset %}

    {% do union_queries.append(query) %}
{% endfor %}

{% if union_queries %}
    {{ union_queries | join('\nunion all\n') }}
    order by column_name
{% else %}
    select
        cast(null as {{ dbt.type_string() }}) as column_name
    where 1=0
{% endif %}
{% endmacro %}
