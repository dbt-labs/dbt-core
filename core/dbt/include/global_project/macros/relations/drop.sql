{% macro drop_relation(relation) -%}
    {{ return(adapter.dispatch('drop_relation', 'dbt')(relation)) }}
{% endmacro %}

{% macro default__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        {%- if relation.is_table -%}
            {{- drop_table(relation) -}}
        {%- elif relation.is_view -%}
            {{- drop_view(relation) -}}
        {%- elif relation.is_materialized_view -%}
            {{- drop_materialized_view(relation) -}}
        {%- else -%}
            drop {{ relation.type }} if exists {{ relation }} cascade
        {%- endif -%}
    {%- endcall %}
{% endmacro %}
