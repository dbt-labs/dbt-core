
{{
	config(
		materialized = "table",
		partition_by = "updated_at",
		cluster_by = "dupe",
	)
}}

select * from {{ ref('view_model') }}
