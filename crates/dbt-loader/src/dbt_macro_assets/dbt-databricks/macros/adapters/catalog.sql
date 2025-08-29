{% macro databricks__get_catalog(database, schemas) -%}

    {% set query %}
        with tables as (
            {{ databricks__get_catalog_tables_sql(database) }}
            {{ databricks__get_catalog_schemas_where_clause_sql(database, schemas) }}
        ),
        columns as (
            {{ databricks__get_catalog_columns_sql(database) }}
            {{ databricks__get_catalog_schemas_where_clause_sql(database, schemas) }}
        )
        {{ databricks__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}
{%- endmacro %}

{% macro databricks__get_catalog_relations(dbschema, relations) -%}

    {% set query %}
        with tables as (
            {{ databricks__get_catalog_tables_sql(dbschema.database) }}
            {{ databricks__get_catalog_relations_where_clause_sql(dbschema.database, relations) }}
        ),
        columns as (
            {{ databricks__get_catalog_columns_sql(dbschema.database) }}
            {{ databricks__get_catalog_relations_where_clause_sql(dbschema.database,relations) }}
        )
        {{ databricks__get_catalog_results_sql() }}
    {%- endset -%}

    {{ return(run_query(query)) }}
{%- endmacro %}

{% macro databricks__get_catalog_tables_sql(database) -%}
    select
        table_catalog as table_database,
        table_schema,
        table_name,
        lower(table_type) as table_type,
        comment as table_comment,
        table_owner,
        'Last Modified' as `stats:last_modified:label`,
        last_altered as `stats:last_modified:value`,
        'The timestamp for last update/change' as `stats:last_modified:description`,
        (last_altered is not null and table_type not ilike '%VIEW%') as `stats:last_modified:include`
    from `system`.`information_schema`.`tables`
{%- endmacro %}

{% macro databricks__get_catalog_columns_sql(database) -%}
    select
        table_catalog as table_database,
        table_schema,
        table_name,
        column_name,
        ordinal_position as column_index,
        lower(full_data_type) as column_type,
        comment as column_comment
    from `system`.`information_schema`.`columns`
{%- endmacro %}

{% macro databricks__get_catalog_results_sql() -%}
    select *
    from tables
    join columns using (table_database, table_schema, table_name)
    order by column_index
{%- endmacro %}

{% macro databricks__get_catalog_schemas_where_clause_sql(catalog, schemas) -%}
    where table_catalog = '{{ catalog|lower }}' and ({%- for relation in schemas -%}
        table_schema = '{{ relation[1]|lower }}'{%- if not loop.last %} or {% endif -%}
    {%- endfor -%})
{%- endmacro %}


{% macro databricks__get_catalog_relations_where_clause_sql(catalog, relations) -%}
    where table_catalog = '{{ catalog|lower }}' and (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    table_schema = '{{ relation.schema|lower }}'
                    and table_name = '{{ relation.identifier|lower }}'
                )
            {% elif relation.schema %}
                (
                    table_schema = '{{ relation.schema|lower }}'
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}
