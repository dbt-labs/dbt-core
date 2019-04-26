{% macro bigquery__archive_hash_arguments(args) %}
  to_hex(md5(concat({% for arg in args %}coalesce(cast({{ arg }} as string), ''){% if not loop.last %}, '|',{% endif %}{% endfor %})))
{% endmacro %}

{% macro bigquery__create_columns(relation, columns) %}
  {{ adapter.alter_table_add_columns(relation, columns) }}
{% endmacro %}

{% macro bigquery__post_archive(staging_relation) %}
  -- Clean up the archive temp table
  {% do drop_relation(staging_relation) %}
{% endmacro %}
