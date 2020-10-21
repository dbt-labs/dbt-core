
{% macro test_number_partitions(model, expected) %}

    {%- set result = get_partitions_metadata(model) %}
    
    {% if result %}
        {% set partitions = result.columns['partition_id'].values() %}
    {% else %}
        {% set partitions = () %}
    {% endif %}
        
    {% set actual = partitions | length %}
    
    {% if model and actual == expected %}
        select 0 as success
    {% else %}
        -- actual: {{ actual }}
        -- expected: {{ expected }}
        select 1 as error
    {% endif %}

{% endmacro %}
