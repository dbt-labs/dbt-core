
{% macro postgres__get_catalog(information_schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}
    {% if (information_schemas | length) != 1 %}
        {{ exceptions.raise_compiler_error('postgres get_catalog requires exactly one database') }}
    {% endif %}
    {% set database = information_schemas[0].database %}
    {{ adapter.verify_database(database) }}

    select
        '{{ database }}' as table_database,
        sch.nspname as table_schema,
        tbl.relname as table_name,
        case tbl.relkind
            when 'v' then 'VIEW'
            else 'BASE TABLE'
        end as table_type,
        null::text as table_comment,
        col.attname as column_name,
        col.attnum as column_index,
        pg_catalog.format_type(col.atttypid, col.atttypmod) as column_type,
        null::text as column_comment,
        pg_get_userbyid(tbl.relowner) as table_owner

    from pg_catalog.pg_namespace sch
    join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
    join pg_catalog.pg_attribute col on col.attrelid = tbl.oid

    where sch.nspname != 'information_schema'
      and sch.nspname not like 'pg_%' -- avoid postgres system schemas
      and not pg_is_other_temp_schema(sch.oid) -- not a temporary schema belonging to another session
      and tbl.relpersistence = 'p' -- [p]ermanent table. Other values are [u]nlogged table, [t]emporary table
      and tbl.relkind in ('r', 'v', 'f', 'p') -- o[r]dinary table, [v]iew, [f]oreign table, [p]artitioned table. Other values are [i]ndex, [S]equence, [c]omposite type, [t]OAST table, [m]aterialized view
      and col.attnum > 0 -- negative numbers are used for system columns such as oid
      and not col.attisdropped -- column as not been dropped

    order by
        sch.nspname,
        tbl.relname,
        col.attnum

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}

{%- endmacro %}
