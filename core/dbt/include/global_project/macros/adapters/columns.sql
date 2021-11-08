{% macro get_columns_in_relation(relation) -%}
  {{ return(adapter.dispatch('get_columns_in_relation', 'dbt')(relation)) }}
{% endmacro %}

{% macro default__get_columns_in_relation(relation) -%}
  {{ exceptions.raise_not_implemented(
    'get_columns_in_relation macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}

{# helper for adapter-specific implementations of get_columns_in_relation #}
{% macro sql_convert_columns_in_relation(table) -%}
  {% set columns = [] %}
  {% for row in table %}
    {% do columns.append(api.Column(*row)) %}
  {% endfor %}
  {{ return(columns) }}
{% endmacro %}


{% macro get_columns_in_query(select_sql) -%}
  {{ return(adapter.dispatch('get_columns_in_query', 'dbt')(select_sql)) }}
{% endmacro %}

{% macro default__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        select * from (
            {{ select_sql }}
        ) as __dbt_sbq
        where false
        limit 0
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}


{% macro alter_column_type(relation, column_name, new_column_type) -%}
  {{ return(adapter.dispatch('alter_column_type', 'dbt')(relation, column_name, new_column_type)) }}
{% endmacro %}

{% macro default__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }} cascade;
    alter table {{ relation }} rename column {{ adapter.quote(tmp_column) }} to {{ adapter.quote(column_name) }}
  {% endcall %}

{% endmacro %}


{% macro alter_relation_add_remove_columns(relation, add_columns = none, remove_columns = none) -%}
  {{ return(adapter.dispatch('alter_relation_add_remove_columns', 'dbt')(relation, add_columns, remove_columns)) }}
{% endmacro %}

{% macro default__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  
  {% if add_columns is none %}
    {% set add_columns = [] %}
  {% endif %}
  {% if remove_columns is none %}
    {% set remove_columns = [] %}
  {% endif %}
  
  {% set sql -%}
     
     alter {{ relation.type }} {{ relation }}
       
            {% for column in add_columns %}
               add column {{ column.name }} {{ column.data_type }}{{ ',' if not loop.last }}
            {% endfor %}{{ ',' if add_columns and remove_columns }}
            
            {% for column in remove_columns %}
                drop column {{ column.name }}{{ ',' if not loop.last }}
            {% endfor %}
  
  {%- endset -%}

  {% do run_query(sql) %}

{% endmacro %}
