{% set columns = adapter.get_columns_in_relation(ref('model')) %}
{% set limit_query = 0 %}
{% if (columns | length) == 0 %}
	{% set limit_query = 1 %}
{% endif %}

select 1 as id limit {{ limit_query }}
