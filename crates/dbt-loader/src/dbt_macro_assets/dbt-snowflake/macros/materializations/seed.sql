-- funcsign: (model, bool, relation, agate_table) -> string
{% macro snowflake__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
        {% set sql = create_csv_table(model, agate_table) %}
        {{ return(sql) }}
    {% else %}
        {# For non-full-refresh, Snowflake uses INSERT OVERWRITE INTO which atomically
           replaces data, so no separate truncate_relation call is needed. #}
        {{ return("") }}
    {% endif %}
{% endmacro %}

-- funcsign: (model, agate_table) -> string
{% macro snowflake__load_csv_rows(model, agate_table) %}
    {% set batch_size = get_batch_size() %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% do adapter.add_query('BEGIN', auto_begin=False) %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% do bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert {% if loop.first %}overwrite {% endif %}into {{ this.render() }} ({{ cols_sql }}) values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    %s
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {% do adapter.add_query('COMMIT', auto_begin=False) %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% materialization seed, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}

    {% set relations = materialization_seed_default() %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return(relations) }}
{% endmaterialization %}
