{% macro test_type_one(model) %}

    select count(*) from (

        select * from {{ model }}
        union all
        select * from {{ ref('model_b') }}
        
    ) as Foo

{% endmacro %}

{% macro test_type_two(model) %}

    {{ config(severity = "WARN") }}

    select count(*) from {{ model }}

{% endmacro %}
