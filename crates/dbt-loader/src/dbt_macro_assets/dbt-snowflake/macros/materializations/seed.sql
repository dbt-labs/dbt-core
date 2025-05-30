{% macro snowflake__load_csv_rows(model, agate_table) %}
    {% set batch_size = get_batch_size() %}
    {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% do bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} ({{ cols_sql }}) values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    %s
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% do adapter.add_query('BEGIN', auto_begin=False) %}
        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}
        {% do adapter.add_query('COMMIT', auto_begin=False) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% materialization seed, adapter='snowflake' %}
    {% set original_query_tag = set_query_tag() %}

    {% set relations = materialization_seed_default() %}

    {% do unset_query_tag(original_query_tag) %}

    {{ return(relations) }}
{% endmaterialization %}
