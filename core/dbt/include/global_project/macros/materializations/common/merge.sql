

{% macro get_merge_sql(target, source, unique_key, dest_columns) -%}
  {{ adapter_macro('get_merge_sql', target, source, unique_key, dest_columns) }}
{%- endmacro %}

{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns) -%}
  {{ adapter_macro('get_delete_insert_merge_sql', target, source, unique_key, dest_columns) }}
{%- endmacro %}


{% macro common_get_merge_sql(target, source, unique_key, dest_columns) -%}
    {%- set dest_cols_csv =  get_quoted_csv(dest_columns | map(attribute="name")) -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE

    {% if unique_key %}
        on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
    {% else %}
        on FALSE
    {% endif %}

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

{%- endmacro %}

{% macro default__get_merge_sql(target, source, unique_key, dest_columns) -%}
    {% set typename = adapter.type() %}

    {{ exceptions.raise_compiler_error(
        'get_merge_sql is not implemented for {}'.format(typename)
       )
    }}

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
