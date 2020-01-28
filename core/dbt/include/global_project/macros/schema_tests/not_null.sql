
{% macro test_not_null(model) %}

{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}

select count(*)
from {{ model }} AS aliased
where aliased.{{ column_name }} is null

{% endmacro %}

