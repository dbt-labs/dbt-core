-- funcsign: (relation, string) -> string
{% macro redshift__get_rename_table_sql(relation, new_name) %}
    alter table {{ relation }} rename to {{ new_name }}
{% endmacro %}


{#--
  Redshift inherits from Postgres but does not support native table partitioning, so it
  must not pick up postgres__rename_relation (which inspects pg_inherits for child
  partitions). Restore the base rename behavior. See dbt-postgres issue #679.
--#}
{% macro redshift__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation.render() }} rename to {{ target_name }}
  {%- endcall %}
{%- endmacro %}
