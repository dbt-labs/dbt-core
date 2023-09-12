{%- macro get_create_sql(relation, sql) -%}
    {{- log('Applying CREATE to: ' ~ relation) -}}
    {{- adapter.dispatch('get_create_sql', 'dbt')(relation, sql) -}}
{%- endmacro -%}


{%- macro default__get_create_sql(relation, sql) -%}

    {%- if relation.is_materialized_view -%}
        {{ get_create_materialized_view_as_sql(relation, sql) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`get_create_sql` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}
