
{{
	config(
		materialized = "table",
		partition_by = "updated_at",
	)
}}

select * from {{ ref('view_model') }}
