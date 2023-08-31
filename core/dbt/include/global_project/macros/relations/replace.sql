{% macro get_replace_sql(existing_relation, target_relation, sql) %}
    {{- log('Applying REPLACE to: ' ~ existing_relation) -}}
    {{- adapter.dispatch('get_replace_sql', 'dbt')(existing_relation, target_relation, sql) -}}
{% endmacro %}


{% macro default__get_replace_sql(existing_relation, target_relation, sql) %}

    {# /* create target_relation as an intermediate relation, then swap it out with the existing one using a backup */ #}
    {%- if target_relation.can_be_renamed and existing_relation.can_be_renamed -%}
        {{ get_create_intermediate_sql(target_relation, sql) }};
        {{ get_create_backup_sql(existing_relation) }};
        {{ get_rename_intermediate_sql(target_relation) }};
        {{ get_drop_backup_sql(existing_relation) }}

    {# /* create target_relation as an intermediate relation, then swap it out with the existing one using drop */ #}
    {%- elif target_relation.can_be_renamed -%}
        {{ get_create_intermediate_sql(target_relation, sql) }};
        {{ get_drop_sql(existing_relation) }};
        {{ get_rename_intermediate_sql(target_relation) }}

    {# /* create target_relation in place by first backing up the existing relation */ #}
    {%- elif existing_relation.can_be_renamed -%}
        {{ get_create_backup_sql(existing_relation) }};
        {{ get_create_sql(target_relation, sql) }};
        {{ get_drop_backup_sql(existing_relation) }}

    {# /* no renaming is allowed, so just drop and create */ #}
    {%- else -%}
        {{ get_drop_sql(existing_relation) }};
        {{ get_create_sql(target_relation, sql) }}

    {%- endif -%}

{% endmacro %}
