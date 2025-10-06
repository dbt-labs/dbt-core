{% macro redshift__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
        RETURNS {{ model.returns.data_type }}
    {# TODO: Stop defaulting to VOLATILE once we have a way to set the volatility #}
    {# We set a default here because redshift requires a volatility to be set #}
    VOLATILE
    AS
{% endmacro %}

{% macro redshift__formatted_scalar_function_args_sql() %}
    {% set args = [] %}
    {% for arg in model.arguments -%}
        {%- do args.append(arg.data_type) -%}
    {%- endfor %}
    {{ args | join(', ') }}
{% endmacro %}
