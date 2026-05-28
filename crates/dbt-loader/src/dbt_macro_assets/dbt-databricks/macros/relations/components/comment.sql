{%- macro get_create_sql_comment(comment) -%}
{% if comment is string -%}
  COMMENT '{{ comment }}'
{%- endif -%}
{%- endmacro -%}
