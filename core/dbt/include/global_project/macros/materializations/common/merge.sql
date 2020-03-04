

{% macro get_merge_sql(target, source, unique_key, dest_columns, predicates=none) -%}
  {{ adapter_macro('get_merge_sql', target, source, unique_key, dest_columns, predicates) }}
{%- endmacro %}


{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
  {{ adapter_macro('get_delete_insert_merge_sql', target, source, unique_key, dest_columns) }}
{%- endmacro %}


{% macro get_insert_overwrite_merge_sql(target, source, dest_columns, predicates) -%}
  {{ adapter_macro('get_insert_overwrite_merge_sql', target, source, dest_columns, predicates) }}
{%- endmacro %}


{% macro default__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched then update set
        {% for column in dest_columns -%}
            {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}


{% macro get_quoted_csv(column_names) %}
    {% set quoted = [] %}
    {% for col in column_names -%}
        {%- do quoted.append(adapter.quote(col)) -%}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}
{% endmacro %}


{% macro common_get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key is not none %}
    delete from {{ target }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ source }}
    );
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    );

{%- endmacro %}

{% macro default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
    {{ common_get_delete_insert_merge_sql(target, source, unique_key, dest_columns) }}
{% endmacro %}


{% macro default__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

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

{% endmacro %}
