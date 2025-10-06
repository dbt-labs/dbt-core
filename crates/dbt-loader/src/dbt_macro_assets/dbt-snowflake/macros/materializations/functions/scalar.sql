{% macro snowflake__scalar_function_create_replace_signature_sql(target_relation) %}
    CREATE OR REPLACE FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql()}})
    RETURNS {{ model.returns.data_type }}
    LANGUAGE SQL
    AS
{% endmacro %}

{% macro snowflake__scalar_function_body_sql() %}
    $$
       {{ compiled_code }}
    $$
{% endmacro %}
