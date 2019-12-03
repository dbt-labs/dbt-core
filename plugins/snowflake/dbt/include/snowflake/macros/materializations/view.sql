{% materialization view, adapter='snowflake' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
