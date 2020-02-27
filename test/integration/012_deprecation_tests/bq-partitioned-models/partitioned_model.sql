
{{
	config(
		materialized = "table",
		partition_by = "updated_at_date",
	)
}}

select

    id,
    current_date as updated_at_date,
    dupe

from {{ ref('data_seed') }}
