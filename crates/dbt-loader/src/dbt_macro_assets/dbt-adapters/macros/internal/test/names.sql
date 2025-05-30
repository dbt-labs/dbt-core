{% macro t_database_name() %}
  {{ return (adapter.dispatch("t_database_name")()) }}
{% endmacro %}

{% macro default__t_database_name() %}
  {{ return(env_var('DBT_DB_NAME')) }}
{% endmacro %}

{% macro bigquery__t_database_name() %}
  {{ return(env_var('GOOGLE_CLOUD_PROJECT')) }}
{% endmacro %}

{% macro databricks__t_database_name() %}
  {{ return(env_var('DATABRICKS_CATALOG')) }}
{% endmacro %}

{% macro redshift__t_database_name() %}
  {{ return(env_var('REDSHIFT_DATABASE')) }}
{% endmacro %}

{% macro t_schema_name() %}
  {{ return(target.schema) }}
{% endmacro %}
