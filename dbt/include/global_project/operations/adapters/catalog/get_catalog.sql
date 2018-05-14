{% operation get_catalog_data %}
    {% set catalog = dbt.get_catalog() %}
    {{ log('ran catalog query:', info=True)}}
    {{ log(catalog, info=True) }}
    {{ return(catalog) }}
{% endoperation %}