{% macro copy_grants() %}
    {{ return(adapter.dispatch('copy_grants', 'dbt')()) }}
{% endmacro %}

{% macro default__copy_grants() %}
    {{ return(True) }}
{% endmacro %}

{% macro should_revoke(existing_relation, full_refresh_mode=True) %}

    {% if not existing_relation %}
        {#-- The table doesn't already exist, so no grants to copy over --#}
        {{ return(False) }}
    {% elif full_refresh_mode %}
        {#-- The object is being REPLACED -- whether grants are copied over depends on the value of user config --#}
        {{ return(copy_grants()) }}
    {% else %}
        {#-- The table is being merged/upserted/inserted -- grants will be carried over --#}
        {{ return(True) }}
    {% endif %}

{% endmacro %}

{% macro get_show_grant_sql(relation) %}
    {{ return(adapter.dispatch("get_show_grant_sql", "dbt")(relation)) }}
{% endmacro %}

{% macro default__get_show_grant_sql(relation) %}
    show grants on {{ relation }}
{% endmacro %}

{% macro get_grant_sql(relation, grant_config) %}
    {{ return(adapter.dispatch('get_grant_sql', 'dbt')(relation, grant_config)) }}
{% endmacro %}

{%- macro default__get_grant_sql(relation, grant_config) -%}
    {%- set grant_statements = [] -%}
    {%- for privilege in grant_config.keys() %}
        {%- set grantees = grant_config[privilege] -%}
        {%- if grantees %}
            {% set grant_sql -%}
                grant {{ privilege }} on {{ relation }} to {{ grantees | join(', ') }}
            {%- endset %}
            {%- do grant_statements.append(grant_sql) -%}
        {% endif -%}
    {%- endfor -%}
    {{ return(grant_statements) }}
{%- endmacro %}

{% macro get_revoke_sql(relation, grant_config) %}
    {{ return(adapter.dispatch("get_revoke_sql", "dbt")(relation, grant_config)) }}
{% endmacro %}

{% macro default__get_revoke_sql(relation, grant_config) %}
    {%- set revoke_statements = [] -%}
    {%- for privilege in grant_config.keys() -%}
        {%- set grantees = grant_config[privilege] -%}
        {%- if grantees %}
            {% set revoke_sql -%}
                revoke {{ privilege }} on {{ relation }} from {{ grantees | join(', ') }}
            {%- endset %}
            {%- do revoke_statements.append(revoke_sql) -%}
        {% endif -%}
    {%- endfor -%}
    {{ return(revoke_statements) }}
{%- endmacro -%}


{% macro call_grant_revoke_statement_list(grant_and_revoke_statement_list) %}
    {{ return(adapter.dispatch("call_grant_revoke_statement_list", "dbt")(grant_and_revoke_statement_list)) }}
{% endmacro %}

{% macro default__call_grant_revoke_statement_list(grant_and_revoke_statement_list) %}
    {% call statement('grants') %}
        {% for grant_or_revoke_statement in grant_and_revoke_statement_list %}
            {{ grant_or_revoke_statement }};
        {% endfor %}
    {% endcall %}
{% endmacro %}


{% macro apply_grants(relation, grant_config, should_revoke) %}
    {{ return(adapter.dispatch("apply_grants", "dbt")(relation, grant_config, should_revoke)) }}
{% endmacro %}

{% macro default__apply_grants(relation, grant_config, should_revoke=True) %}
    {% if grant_config %}
        {% if should_revoke %}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = diff_of_two_dicts(grant_config, current_grants_dict) %}
            {% set needs_revoking = diff_of_two_dicts(current_grants_dict, grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {% set needs_revoking = {} %}
            {% set needs_granting = grant_config %}
        {% endif %}
        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_revoke_sql(relation, needs_revoking) %}
            {% set grant_statement_list = get_grant_sql(relation, needs_granting) %}
            {% set grant_and_revoke_statement_list = revoke_statement_list + grant_statement_list %}
            {% if grant_and_revoke_statement_list %}
                {{ call_grant_revoke_statement_list(grant_and_revoke_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
