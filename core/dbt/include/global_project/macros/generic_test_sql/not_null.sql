{% macro default__test_not_null(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null

{% endmacro %}

