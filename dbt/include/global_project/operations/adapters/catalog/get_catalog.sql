{% operation get_catalog_data %}
    {% set catalog = dbt.get_catalog() %}
    {{ log(catalog, info=True) }}
    {{ return(catalog) }}
{% endoperation %}