{% macro bigquery__create_temporary_table(sql, relation) %}
    {% set tmp_relation = adapter.create_temporary_table(sql) %}
    {{ return(tmp_relation) }}
{% endmacro %}


{% macro bigquery__archive_scd_hash() %}
    to_hex(md5(concat(cast(`dbt_pk` as string), '|', cast(`dbt_updated_at` as string))))
{% endmacro %}


{% macro bigquery__create_columns(relation, columns) %}
  {{ adapter.alter_table_add_columns(relation, columns) }}
{% endmacro %}


{% macro bigquery__archive_update(target_relation, tmp_relation) %}
    update {{ target_relation }} as dest
    set dest.{{ adapter.quote('valid_to') }} = tmp.{{ adapter.quote('valid_to') }}
    from {{ tmp_relation }} as tmp
    where tmp.{{ adapter.quote('scd_id') }} = dest.{{ adapter.quote('scd_id') }}
      and {{ adapter.quote('change_type') }} = 'update';
{% endmacro %}
