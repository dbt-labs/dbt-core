

{% macro get_merge_sql(target, source, unique_key, dest_columns, predicates=none, incrementnal_predicates=none) -%}
  {{ adapter.dispatch('get_merge_sql')(target, source, unique_key, dest_columns, predicates, incremental_predicates) }}
{%- endmacro %}


{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
  {{ adapter.dispatch('get_delete_insert_merge_sql')(target, source, unique_key, dest_columns, incremental_predicates) }}
{%- endmacro %}


{% macro get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header=false, incremental_predicates=none) -%}
  {{ adapter.dispatch('get_insert_overwrite_merge_sql')(target, source, dest_columns, predicates, include_sql_header, incremental_predicates) }}
{%- endmacro %}


{% macro default__get_merge_sql(target, source, unique_key, dest_columns, predicates, incremental_predictates=none) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set incremental_predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

    {% if incremental_predicates %} where {{ incremental_predicates | join(' and ') }} {% endif %}

{% endmacro %}


{% macro get_quoted_csv(column_names) %}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(adapter.quote(col)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{% endmacro %}


{% macro common_get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set incremental_predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}

    {% if unique_key is not none %}
    delete from {{ target }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ source }}
    )
    {% if incremental_predicates %} and {{ incremental_predicates | join(' and ') }} {% endif %};
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    );

{%- endmacro %}

{% macro default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {{ common_get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) }}
{% endmacro %}


{% macro default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header, incremental_predicates=none) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set incremental_predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}

    {{ sql_header if sql_header is not none and include_sql_header }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
        {% if predicates %} and {{ predicates | join(' and ') }} {% endif %}
        then delete

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

    {% if incremental_predicates %} where {{ incremental_predicates | join(' and ') }} {% endif %}

{% endmacro %}
