{# /*
This was already implemented. Instead of creating a new macro that aligns with the standard,
this was reused and the default was maintained. This gets called by `drop_relation`, which
actually executes the drop, and `get_drop_sql`, which returns the template.
*/ #}

{% macro drop_view(relation) -%}
    {{ return(adapter.dispatch('drop_view', 'dbt')(relation)) }}
{%- endmacro %}


{% macro default__drop_view(relation) -%}
    drop view if exists {{ relation }} cascade
{%- endmacro %}
