{% operation get_relations_data %}
    {% set relations = dbt.get_relations() %}
    {{ return(relations) }}
{% endoperation %}
