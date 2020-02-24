
{{
	config(
		materialized = "table",
		partition_by = "updated_at_date",
		cluster_by = ["dupe","id"],
	)
}}

select

    id,
    current_date as updated_at_date,
    dupe

from {{ ref('data_seed') }}
