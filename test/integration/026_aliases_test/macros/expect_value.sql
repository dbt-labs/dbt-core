
-- cross-db compatible test, similar to accepted_values

{% macro test_expect_value(model, field, value) %}

select count(*)
from {{ model }}
where {{ field }} != '{{ value }}'

{% endmacro %}
