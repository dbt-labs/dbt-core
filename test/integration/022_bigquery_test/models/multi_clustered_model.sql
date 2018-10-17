
{{
	config(
		materialized = "table",
		partition_by = "updated_at",
		cluster_by = ["dupe","id"],
	)
}}

select * from {{ ref('view_model') }}
