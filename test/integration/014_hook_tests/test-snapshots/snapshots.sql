{% snapshot example_snapshot %}
{{
	config(target_schema=schema, unique_key='a', strategy='check', check_cols='all')
}}
	select * from {{ ref('example_seed') }}
{% endsnapshot %}
