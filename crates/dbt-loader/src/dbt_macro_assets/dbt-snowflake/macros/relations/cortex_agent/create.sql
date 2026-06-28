{# -----------------------------------------------------------------------
   CREATE OR REPLACE AGENT DDL builder.

   Dispatched so a project may override it, and so non-Snowflake adapters
   fail with a clear "not implemented" message rather than emitting bad DDL.
   Snowflake DDL reference:
     https://docs.snowflake.com/en/sql-reference/sql/create-agent
   ----------------------------------------------------------------------- #}

{% macro get_create_cortex_agent_sql(relation, specification, comment='', profile=none) -%}
  {{ return(adapter.dispatch('get_create_cortex_agent_sql', 'dbt')(relation, specification, comment, profile)) }}
{%- endmacro %}


{% macro default__get_create_cortex_agent_sql(relation, specification, comment='', profile=none) -%}
  {{ exceptions.raise_compiler_error(
      'get_create_cortex_agent_sql is not implemented for adapter type: ' ~ adapter.type()
  ) }}
{%- endmacro %}


{% macro snowflake__get_create_cortex_agent_sql(relation, specification, comment='', profile=none) -%}

  {%- set profile_json = none -%}
  {%- if profile is not none and profile | length > 0 -%}
    {#-- tojson produces compact JSON; single-quote-escape for SQL embedding --#}
    {%- set profile_json = profile | tojson -%}
  {%- endif -%}

  CREATE OR REPLACE AGENT {{ relation }}
  {%- if comment | trim != '' %}
  COMMENT = {{ "'" ~ comment | replace("'", "''") ~ "'" }}
  {%- endif %}
  {%- if profile_json is not none %}
  PROFILE = '{{ profile_json | replace("'", "''") }}'
  {%- endif %}
  FROM SPECIFICATION $$
{{ specification }}
  $$

{%- endmacro %}
