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
        {%- set snowflake_initialization_warehouse = configuration_changes.snowflake_initialization_warehouse -%}
        {%- if snowflake_initialization_warehouse and snowflake_initialization_warehouse.context -%}{{- log('Applying UPDATE INITIALIZATION_WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set immutable_where = configuration_changes.immutable_where -%}
        {%- if immutable_where and immutable_where.context -%}{{- log('Applying UPDATE IMMUTABLE WHERE to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set cluster_by = configuration_changes.cluster_by -%}
        {%- if cluster_by and cluster_by.context -%}{{- log('Applying UPDATE CLUSTER BY to: ' ~ existing_relation) -}}{%- endif -%}

        {#- Determine what SET changes we have -#}
        {%- set has_set_changes = target_lag or snowflake_warehouse or (snowflake_initialization_warehouse and snowflake_initialization_warehouse.context) -%}

        {#- Handle SET operations -#}
        {% if has_set_changes %}
        alter dynamic table {{ existing_relation }} set
            {% if target_lag and target_lag.context %}target_lag = '{{ target_lag.context }}'{% endif %}
            {% if snowflake_warehouse %}warehouse = {{ snowflake_warehouse.context }}{% endif %}
            {% if snowflake_initialization_warehouse and snowflake_initialization_warehouse.context %}initialization_warehouse = {{ snowflake_initialization_warehouse.context }}{% endif %}
        {% endif %}

        {#- Handle unsetting initialization_warehouse when changed to None/empty -#}
        {% if snowflake_initialization_warehouse and not snowflake_initialization_warehouse.context %}
        {% if has_set_changes %};{% endif %}
        alter dynamic table {{ existing_relation }} unset initialization_warehouse
        {% endif %}

        {#- Handle setting or unsetting immutable_where -#}
        {% if immutable_where %}
        {%- set needs_semicolon = has_set_changes or (snowflake_initialization_warehouse and not snowflake_initialization_warehouse.context) -%}
        {% if needs_semicolon %};{% endif %}
        {% if immutable_where.context %}
        alter dynamic table {{ existing_relation }} set immutable where ({{ immutable_where.context }})
        {% else %}
        alter dynamic table {{ existing_relation }} unset immutable where
        {% endif %}
        {% endif %}

        {#- Track if we've had any previous ALTER statements for semicolon placement -#}
        {%- set has_prior_statements = has_set_changes or (snowflake_initialization_warehouse and not snowflake_initialization_warehouse.context) or immutable_where -%}

        {#- Handle CLUSTER BY changes (add/modify) -#}
        {% if cluster_by and cluster_by.context %}
        {% if has_prior_statements %};{% endif %}
        alter dynamic table {{ existing_relation }} cluster by ({{ cluster_by.context }})
        {% endif %}

        {#- Handle DROP CLUSTERING KEY when cluster_by is removed -#}
        {% if cluster_by and not cluster_by.context %}
        {%- if cluster_by -%}{{- log('Applying DROP CLUSTERING KEY to: ' ~ existing_relation) -}}{%- endif -%}
        {% if has_prior_statements %};{% endif %}
        alter dynamic table {{ existing_relation }} drop clustering key
        {% endif %}

    {%- endif -%}

{%- endmacro %}
