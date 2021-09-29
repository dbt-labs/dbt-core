{% macro my_create_schema(db_name, schema_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name).without_identifier() %}
    {% do create_schema(relation) %}
{% endmacro %}

{% macro my_drop_schema(db_name, schema_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name).without_identifier() %}
    {% do drop_schema(relation) %}
{% endmacro %}


{% macro my_create_table_as(db_name, schema_name, table_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name, identifier=table_name) %}
    {% do run_query(create_table_as(false, relation, 'select 1 as id')) %}
{% endmacro %}


{% macro ensure_one_relation_in(db_name, schema_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name).without_identifier() %}
    {% set results = list_relations_without_caching(relation) %}
    {% set rlen = (results | length) %}
    {% if rlen != 1 %}
        {% do exceptions.raise_compiler_error('Incorect number of results (expected 1): ' ~ rlen) %}
    {% endif %}
    {% set result = results[0] %}
    {% set columns = get_columns_in_relation(result) %}
    {% set clen = (columns | length) %}
    {% if clen != 1 %}
        {% do exceptions.raise_compiler_error('Incorrect number of columns (expected 1): ' ~ clen) %}
    {% endif %}
{% endmacro %}
