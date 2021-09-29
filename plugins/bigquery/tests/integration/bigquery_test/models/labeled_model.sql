{{
	config(
		materialized = "table",
		labels = {'town': 'fish', 'analytics': 'yes'}
	)
}}

select * from {{ ref('view_model') }}
