-- funcsign: (relation, snowflake_node_config, relation, string) -> string
{% macro snowflake__get_alter_dynamic_table_as_sql(
    existing_relation,
    configuration_changes,
    target_relation,
    sql
) -%}
    {{- log('Applying ALTER to: ' ~ existing_relation) -}}

    {% if configuration_changes.requires_full_refresh %}
        {{- get_replace_sql(existing_relation, target_relation, sql) -}}

    {% else %}

        {%- set target_lag = configuration_changes.target_lag -%}
        {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set snowflake_warehouse = configuration_changes.snowflake_warehouse -%}
        {%- if snowflake_warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}

        alter dynamic table {{ existing_relation }} set
            {% if target_lag %}target_lag = '{{ target_lag.context }}'{% endif %}
            {% if snowflake_warehouse %}warehouse = {{ snowflake_warehouse.context }}{% endif %}

    {%- endif -%}

{%- endmacro %}
