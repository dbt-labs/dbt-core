{% macro redshift__get_catalog(information_schema, schemas) %}

    {% set database = information_schema.database %}
    {{ adapter.verify_database(database) }}

    {#-- Compute a left-outer join in memory. Some Redshift queries are
      -- leader-only, and cannot be joined to other compute-based queries #}

    {% set catalog = _redshift__get_base_catalog_by_schema(database, schemas) %}

    {% set select_extended = redshift__can_select_from('svv_table_info') %}
    {% if select_extended %}
        {% set extended_catalog = _redshift__get_extended_catalog_by_schema(schemas) %}
        {% set catalog = catalog.join(extended_catalog, ['table_schema', 'table_name']) %}
    {% else %}
        {{ redshift__no_svv_table_info_warning() }}
    {% endif %}

    {{ return(catalog) }}

{% endmacro %}


{% macro _redshift__get_base_catalog_by_schema(database, schemas) -%}
    {%- call statement('base_catalog', fetch_result=True) -%}
        with
            late_binding as ({{ _redshift__get_late_binding_by_schema_sql(schemas) }}),
            early_binding as ({{ _redshift__get_early_binding_by_schema_sql(database, schemas) }}),
            unioned as (select * from early_binding union all select * from late_binding),
            table_owners as ({{ redshift__get_table_owners_sql() }})
        select '{{ database }}' as table_database, *
        from unioned
        join table_owners using (table_schema, table_name)
        order by "column_index"
    {%- endcall -%}
    {{ return(load_result('base_catalog').table) }}
{%- endmacro %}


{% macro _redshift__get_late_binding_by_schema_sql(schemas) %}
    {{ redshift__get_late_binding_sql() }}
    where (
        {%- for schema in schemas -%}
            upper(table_schema) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{% endmacro %}


{% macro _redshift__get_early_binding_by_schema_sql(database, schemas) %}
    {{ redshift__get_early_binding_sql(database) }}
    and (
        {%- for schema in schemas -%}
            upper(sch.nspname) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{% endmacro %}


{% macro _redshift__get_extended_catalog_by_schema(schemas) %}
    {%- call statement('extended_catalog', fetch_result=True) -%}
        {{ redshift__get_extended_catalog_sql() }}
        where (
            {%- for schema in schemas -%}
                upper("schema") = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
            {%- endfor -%}
        )
    {%- endcall -%}
    {{ return(load_result('extended_catalog').table) }}
{% endmacro %}
