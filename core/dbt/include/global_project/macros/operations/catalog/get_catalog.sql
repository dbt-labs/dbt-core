{% operation get_catalog_data %}
    {% set catalog = dbt.get_catalog() %}
    {{ return(catalog) }}
{% endoperation %}
