{% macro snowflake__test_aggregated_unique(model, column_names) %}
{% set skip_column_names = aggregated_test_skip_column_names | default([]) %}
{% set filtered_columns = [] %}

{% for column_name in column_names %}
    {% if column_name not in skip_column_names %}
        {% do filtered_columns.append(column_name) %}
    {% endif %}
{% endfor %}
{% if filtered_columns %}
select
    case
        {%- for column_name in filtered_columns %}
        when grouping({{ column_name }}) = 0 then '{{ column_name }}'
        {%- endfor %}
    end as column_name,
    coalesce({{ filtered_columns | join(', ') }}) as unique_field,
    count(*) as n_records
from {{ model }}
where {{ filtered_columns | join(' is not null or ') }} is not null
group by grouping sets (
    {%- for column_name in filtered_columns -%}
    ({{ column_name }})
    {%- if not loop.last -%}, {% endif -%}
    {%- endfor -%}
)
having count(*) > 1
   and coalesce({{ filtered_columns | join(', ') }}) is not null
{% else %}
select
    cast(null as {{ dbt.type_string() }}) as column_name,
    cast(null as {{ dbt.type_string() }}) as unique_field,
    cast(null as {{ dbt.type_int() }}) as n_records
where 1 = 0
{% endif %}
{% endmacro %}