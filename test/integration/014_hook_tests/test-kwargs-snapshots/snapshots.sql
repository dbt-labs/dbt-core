{% snapshot example_snapshot %}
{{
	config(
		target_schema=schema,
		unique_key='a',
		strategy='check',
		check_cols='all',
		post_hook='alter table {{ this }} add column new_col int')
}}
{{
	config(post_hook='update {{ this }} set new_col = 1')
}}
	select * from {{ ref('example_seed') }}
{% endsnapshot %}
