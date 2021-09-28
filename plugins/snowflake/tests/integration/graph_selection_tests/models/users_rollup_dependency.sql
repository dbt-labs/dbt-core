{{
	config(materialized='table')
}}

select * from {{ ref('users_rollup') }}
