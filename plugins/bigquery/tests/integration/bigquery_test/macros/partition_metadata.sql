
{% test number_partitions(model, expected) %}

    {%- set result = get_partitions_metadata(model) %}

    {% if result %}
        {% set partitions = result.columns['partition_id'].values() %}
    {% else %}
        {% set partitions = () %}
    {% endif %}

    {% set actual = partitions | length %}
    {% set success = 1 if model and actual == expected else 0 %}

    select 'Expected {{ expected }}, but got {{ actual }}' as validation_error
    from (select true)
    where {{ success }} = 0

{% endtest %}
