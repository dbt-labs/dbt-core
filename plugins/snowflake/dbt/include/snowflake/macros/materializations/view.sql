{% materialization view, adapter='snowflake' -%}
    {{ create_or_replace_view() }}
{%- endmaterialization %}
