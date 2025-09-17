
{% macro salesforce__create_csv_table(model, agate_table) %}
    -- noop
{% endmacro %}

{% macro salesforce__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro salesforce__load_csv_rows(model, agate_table) %}
    {%- set delimiter = model['config'].get('delimiter', ',') -%}
    {# only comma delimiter is supported for salesforce #}
    {{ adapter.load_dataframe(
        model['database'],
        model['schema'],
        model['alias'],
        model['project_root'] | string ~ model['original_file_path'] | string,
        agate_table,
        ',',
    ) }}
{% endmacro %}
