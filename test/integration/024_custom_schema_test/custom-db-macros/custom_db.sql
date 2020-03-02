
{% macro generate_database_name(database_name, node) %}
    {% if database_name == 'alt' %}
        {{ env_var('SNOWFLAKE_TEST_ALT_DATABASE') }}
    {% elif database_name %}
        {{ database_name }}
    {% else %}
        {{ target.database }}
    {% endif %}
{% endmacro %}
