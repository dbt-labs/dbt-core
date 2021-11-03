{% test type_one(model) %}

    select * from (

        select * from {{ model }}
        union all
        select * from {{ ref('model_b') }}

    ) as Foo

{% endtest %}

{% test type_two(model) %}

    {{ config(severity = "ERROR") }}

    select * from {{ model }}

{% endtest %}
