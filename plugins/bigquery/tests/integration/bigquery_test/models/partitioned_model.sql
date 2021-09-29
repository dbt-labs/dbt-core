
{{
	config(
		materialized = "table",
		partition_by = {'field': 'updated_at', 'data_type': 'date'},
	)
}}

select * from {{ ref('view_model') }}
