{% macro postgres__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}

    -- parse out all changes that can be applied via alter
    {%- set indexes = configuration_changes.pop("indexes", []) -%}

    -- if there are no remaining changes, then all changes can be implemented via alter
    {%- if configuration_changes == {} -%}

        {{- return(postgres__update_indexes_on_materialized_view(relation, indexes)) -}}

    -- otherwise, a full refresh is needed
    {%- else -%}
        {{- return(get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation)) -}}

    {%- endif -%}

{% endmacro %}


{% macro postgres__get_create_materialized_view_as_sql(relation, sql) %}
    create materialized view if not exists {{ relation }} as {{ sql }};
{% endmacro %}


{% macro postgres__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{- get_create_materialized_view_as_sql(intermediate_relation, sql) -}}

    {% if existing_relation is not none %}
        alter materialized view {{ existing_relation }} rename to {{ backup_relation.include(database=False, schema=False) }};
    {% endif %}

    alter materialized view {{ intermediate_relation }} rename to {{ relation.include(database=False, schema=False) }};

{% endmacro %}


{% macro postgres__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set existing_indexes = run_query(get_show_indexes_sql(existing_relation)) %}
    {% set index_updates = existing_relation.get_index_updates(existing_indexes, new_config) %}

    {% set _configuration_changes = {} %}

    {% if index_updates %}
        {% set _dummy = _configuration_changes.update({"indexes": index_updates}) %}
    {% endif %}

    {% do return(_configuration_changes) %}
{% endmacro %}


{% macro postgres__refresh_materialized_view(relation) %}
    refresh materialized view {{ relation }};
{% endmacro %}


{% macro postgres__update_indexes_on_materialized_view(relation, index_updates) %}
    {{ log("Applying UPDATE INDEXES to: " ~ relation) }}

    {% for _index_update in index_updates %}
        {% set _action = _index_update.get("action") %}

        {% if _action == "drop" %}
            {% set _index_name = _index_update.get("context") %}
            {{ postgres__get_drop_index_sql(relation, _index_name) }}

        {% elif _action == "create" %}
            {% set _index_dict = _index_update.get("context") %}
            {{ postgres__get_create_index_sql(relation, _index_dict) }}

        {% else %}
            {{ exceptions.raise_compiler_error(
                "Unsupported action supplied to postgres__update_indexes_on_materialized_view: " ~ _action)
            }}

        {% endif %}
    {% endfor %}
{% endmacro %}
