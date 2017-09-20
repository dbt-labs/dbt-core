
{% macro test_not_null(model, arg) %}

select count(*)
from {{ model }}
where {{ arg }} is null

{% endmacro %}

