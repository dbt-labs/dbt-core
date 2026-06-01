{%- macro redshift__drop_view(relation) -%}
    {#- DIVERGENCE: Fusion gates the cascade clause on adapter.has_feature, which is
        Fusion-only. v1 dbt-redshift always cascades; mirror that under dbt-core (1.x). -#}
    drop view if exists {{ relation }}{% if not dbt_version.startswith('2.') or not adapter.has_feature('drop_without_cascade') %} cascade{% endif %}
{%- endmacro -%}
