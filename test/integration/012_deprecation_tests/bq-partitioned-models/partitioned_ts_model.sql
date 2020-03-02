
{{
	config(
		materialized = "table",
		partition_by = "date(updated_at_ts)",
	)
}}

select

    id,
    current_timestamp as updated_at_ts,
    dupe

from {{ ref('data_seed') }}
