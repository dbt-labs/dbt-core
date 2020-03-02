
{{
	config(
		materialized = "table",
		partition_by = {"field": "updated_at", "data_type": "date"},
		cluster_by = "dupe",
	)
}}

select * from {{ ref('view_model') }}
