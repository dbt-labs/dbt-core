-- funcsign: (relation, list[string]) -> string
{% macro default__test_aggregated_unique(model, column_names) %}

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
        {{ dbt.string_literal(column_name) }} as column_name,
        {{ safe_cast(column_name, dbt.type_string()) }} as unique_field,
        count(*) as n_records
    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) > 1
    {% endset %}

    {% do union_queries.append(query) %}
{% endfor %}

{% if union_queries %}
    {{ union_queries | join('\nunion all\n') }}
    order by column_name, n_records desc
{% else %}
    select
        cast(null as {{ dbt.type_string() }}) as column_name,
        cast(null as {{ dbt.type_string() }}) as unique_field,
        cast(null as {{ dbt.type_int() }}) as n_records
    where 1=0
{% endif %}

{% endmacro %}
