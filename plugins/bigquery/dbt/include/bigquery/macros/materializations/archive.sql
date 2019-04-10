{% macro bigquery__create_temporary_table(sql, relation) %}
    {% set tmp_relation = adapter.create_temporary_table(sql) %}
    {{ return(tmp_relation) }}
{% endmacro %}


{% macro bigquery__archive_hash_arguments(args) %}
  to_hex(md5(concat({% for arg in args %}coalesce(cast({{ arg }} as string), ''){% if not loop.last %}, '|',{% endif %}{% endfor %})))
{% endmacro %}

{% macro bigquery__create_columns(relation, columns) %}
  {{ adapter.alter_table_add_columns(relation, columns) }}
{% endmacro %}


{% macro bigquery__archive_update(target_relation, tmp_relation) %}
    update {{ target_relation }} as dest
    set dest.dbt_valid_to = tmp.dbt_valid_to
    from {{ tmp_relation }} as tmp
    where tmp.dbt_scd_id = dest.dbt_scd_id
      and change_type = 'update';
{% endmacro %}
