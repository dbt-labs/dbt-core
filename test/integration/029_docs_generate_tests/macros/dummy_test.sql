
{% test nothing(model) %}

-- a silly test to make sure that table-level tests show up in the manifest
-- without a column_name field
select 0

{% endtest %}

